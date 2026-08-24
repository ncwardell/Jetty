package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"
)

// Passphrase-wrapped backup format:
//
//	[12 bytes magic "JETTY-ENC-V1"][16 byte salt][12 byte nonce][ciphertext]
//
// where ciphertext = AES-256-GCM(passphrase-derived-key, nonce, tar.gz).
// Argon2id parameters are picked for "wait a couple of seconds on
// modern hardware" - this is offline-resistant by design since the
// backup ends up on disk somewhere.
var backupEncMagic = []byte("JETTY-ENC-V1")

const (
	backupSaltSize            = 16
	backupNonceSize           = 12 // AES-GCM standard
	backupArgonTime    uint32 = 3
	backupArgonMemory  uint32 = 64 * 1024
	backupArgonThreads uint8  = 4
	backupArgonKeyLen  uint32 = 32
)

// deriveBackupKey runs Argon2id over (passphrase, salt) for a 32-byte AES key.
func deriveBackupKey(passphrase string, salt []byte) []byte {
	return argon2.IDKey([]byte(passphrase), salt,
		backupArgonTime, backupArgonMemory, backupArgonThreads, backupArgonKeyLen)
}

// encryptBackup wraps plain in JETTY-ENC-V1 format. Caller-supplied
// passphrase is run through Argon2id with a fresh per-backup salt.
func encryptBackup(plain []byte, passphrase string) ([]byte, error) {
	salt := make([]byte, backupSaltSize)
	if _, err := io.ReadFull(cryptorand.Reader, salt); err != nil {
		return nil, fmt.Errorf("salt: %w", err)
	}
	nonce := make([]byte, backupNonceSize)
	if _, err := io.ReadFull(cryptorand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	key := deriveBackupKey(passphrase, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plain, nil)

	out := make([]byte, 0, len(backupEncMagic)+len(salt)+len(nonce)+len(ciphertext))
	out = append(out, backupEncMagic...)
	out = append(out, salt...)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

// decryptBackup is the inverse of encryptBackup. Returns the
// plaintext (tar.gz bytes) or an error if the magic doesn't match,
// the passphrase is wrong, etc.
func decryptBackup(blob []byte, passphrase string) ([]byte, error) {
	if !bytes.HasPrefix(blob, backupEncMagic) {
		return nil, fmt.Errorf("not a passphrase-wrapped backup")
	}
	if len(blob) < len(backupEncMagic)+backupSaltSize+backupNonceSize {
		return nil, fmt.Errorf("backup truncated")
	}
	off := len(backupEncMagic)
	salt := blob[off : off+backupSaltSize]
	off += backupSaltSize
	nonce := blob[off : off+backupNonceSize]
	off += backupNonceSize
	ciphertext := blob[off:]

	key := deriveBackupKey(passphrase, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed (wrong passphrase?): %w", err)
	}
	return plain, nil
}

// =============================================================================
// HTTP Handlers - cluster state backup and restore
// =============================================================================
//
// state.json + the compose dir are the only things on disk that aren't
// regenerable. WARP and Cloudflare tunnel registrations live in /data/warp
// (also captured) - those, plus your secret, get the cluster back.
//
//   GET  /api/backup    download a tar.gz containing state.json, compose/, warp/
//   POST /api/restore   upload a tar.gz; replaces state.json and compose dir
//
// Restore is destructive on the receiving node: it overwrites state.json
// and the compose directory. Apply with care. Typically used to seed a
// rebuilt node with the cluster's last-known configuration.

// apiBackup godoc
// @Summary Download a backup of cluster state
// @Description Returns a tar.gz of state.json + compose dir + warp dir. If X-Backup-Passphrase header is set, the tar.gz is wrapped with Argon2id+AES-GCM into the JETTY-ENC-V1 format - safe to share/store. Without the header, output is plain tar.gz (full credentials in the clear).
// @Tags backup
// @Param X-Backup-Passphrase header string false "Optional passphrase. If present, the response body is JETTY-ENC-V1 wrapped instead of plain tar.gz."
// @Produce application/gzip
// @Success 200 {file} file "tar.gz archive (or JETTY-ENC-V1 blob if passphrase set)"
// @Router /backup [get]
func (a *Agent) apiBackup(w http.ResponseWriter, r *http.Request) {
	// Backup contains state.json which now carries AdminKey,
	// EncryptionKey, SelfAPIKey, and every peer's APIKey. Hand-out to
	// a peer key would let that peer recover admin credentials. Admin
	// only.
	if !a.adminAuthorize(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized: admin key required for backup")
		return
	}

	passphrase := r.Header.Get("X-Backup-Passphrase")
	a.writeBackup(w, passphrase, fmt.Sprintf("jetty-backup-%s.tar.gz", time.Now().UTC().Format("20060102-150405")))
}

// writeBackup is the shared backup-emitter. Used by apiBackup and by
// the scheduled backup goroutine. When passphrase is empty, streams
// plain tar.gz to w. When set, builds the tar.gz into a buffer,
// wraps with JETTY-ENC-V1 (Argon2id+AES-GCM), and writes the result.
//
// w may be a stand-in (e.g. os.File-wrapping ResponseWriter) when
// called from the scheduled-backup path. filename is purely for the
// Content-Disposition header on HTTP responses.
func (a *Agent) writeBackup(w http.ResponseWriter, passphrase, filename string) {
	a.saveState()

	if passphrase == "" {
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, filename))
		_ = a.streamBackupTarGz(w)
		return
	}

	// Encrypted path: build tar.gz in-memory, wrap, write.
	var buf bytes.Buffer
	if err := a.streamBackupTarGz(&buf); err != nil {
		log.Printf("Backup: build failed: %v", err)
		writeError(w, http.StatusInternalServerError, "backup build failed: "+err.Error())
		return
	}
	wrapped, err := encryptBackup(buf.Bytes(), passphrase)
	if err != nil {
		log.Printf("Backup: encrypt failed: %v", err)
		writeError(w, http.StatusInternalServerError, "backup encrypt failed: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename=%q`, strings.TrimSuffix(filename, ".gz")+".enc"))
	_, _ = w.Write(wrapped)
}

// streamBackupTarGz writes the tar.gz of state.json + compose + warp
// to dst. Used by both the live HTTP handler (writing directly to the
// response) and the encrypted path (writing to a buffer first).
func (a *Agent) streamBackupTarGz(dst io.Writer) error {
	gz := gzip.NewWriter(dst)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	if err := writeFileToTar(tw, filepath.Join(a.dataDir, "state.json"), "state.json"); err != nil {
		log.Printf("Backup: failed to add state.json: %v", err)
	}
	if err := writeFileToTar(tw, filepath.Join(a.dataDir, "hwid"), "hwid"); err != nil {
		log.Printf("Backup: failed to add hwid: %v", err)
	}
	if err := writeDirToTar(tw, a.composeDir, "compose"); err != nil {
		log.Printf("Backup: failed to add compose dir: %v", err)
	}
	warpDir := filepath.Join(a.dataDir, "warp")
	if _, err := os.Stat(warpDir); err == nil {
		if err := writeDirToTar(tw, warpDir, "warp"); err != nil {
			log.Printf("Backup: failed to add warp dir: %v", err)
		}
	}
	return nil
}

// apiRestore godoc
// @Summary Restore cluster state from a backup
// @Description Accepts a tar.gz produced by /api/backup and overwrites state.json and the compose directory. Destructive. Restart the agent after restore to pick up the new state.
// @Tags backup
// @Accept application/gzip
// @Produce json
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "invalid archive"
// @Router /restore [post]
func (a *Agent) apiRestore(w http.ResponseWriter, r *http.Request) {
	// Restore overwrites every credential on this node. Admin only.
	if !a.adminAuthorize(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized: admin key required for restore")
		return
	}

	defer r.Body.Close()

	// Stage to a buffer first so a partial/broken upload doesn't half-write
	// our state. For typical clusters the backup is small (KBs to a few MB),
	// so buffering in memory is fine. Cap at 64 MB to avoid OOM from a
	// malicious uploader.
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, io.LimitReader(r.Body, 64<<20)); err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	// If the body is the encrypted JETTY-ENC-V1 format, unwrap it
	// using the passphrase from X-Backup-Passphrase. Symmetric with
	// the encrypted-write path in apiBackup.
	body := buf.Bytes()
	if bytes.HasPrefix(body, backupEncMagic) {
		passphrase := r.Header.Get("X-Backup-Passphrase")
		if passphrase == "" {
			writeError(w, http.StatusBadRequest, "encrypted backup detected; X-Backup-Passphrase header required")
			return
		}
		plain, err := decryptBackup(body, passphrase)
		if err != nil {
			writeError(w, http.StatusBadRequest, "decrypt: "+err.Error())
			return
		}
		buf = bytes.NewBuffer(plain)
	}

	gz, err := gzip.NewReader(buf)
	if err != nil {
		writeError(w, http.StatusBadRequest, "decompress: "+err.Error())
		return
	}
	defer gz.Close()
	tr := tar.NewReader(gz)

	// First pass: validate that this looks like a Jetty backup before
	// touching disk. Specifically, expect state.json at the archive root.
	tmpDir, err := os.MkdirTemp(a.dataDir, "restore-*")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "mkdir temp: "+err.Error())
		return
	}
	defer os.RemoveAll(tmpDir)

	stateFound := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "tar read: "+err.Error())
			return
		}
		// Refuse path-traversal attempts. Tar entries should all be
		// relative paths under the archive root.
		clean := filepath.Clean(hdr.Name)
		if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
			writeError(w, http.StatusBadRequest, "archive contains unsafe path: "+hdr.Name)
			return
		}
		target := filepath.Join(tmpDir, clean)
		// Defense in depth: ensure target is under tmpDir.
		if !strings.HasPrefix(target, tmpDir+string(os.PathSeparator)) && target != tmpDir {
			writeError(w, http.StatusBadRequest, "archive escapes target dir: "+hdr.Name)
			return
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0700); err != nil {
				writeError(w, http.StatusInternalServerError, "mkdir: "+err.Error())
				return
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				writeError(w, http.StatusInternalServerError, "mkdir parent: "+err.Error())
				return
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "open: "+err.Error())
				return
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				writeError(w, http.StatusInternalServerError, "write: "+err.Error())
				return
			}
			f.Close()
			if clean == "state.json" {
				stateFound = true
			}
		default:
			// Skip symlinks, devices, etc. - we don't expect any.
			continue
		}
	}

	if !stateFound {
		writeError(w, http.StatusBadRequest, "archive does not contain state.json - not a Jetty backup?")
		return
	}

	// Validate that state.json parses before we commit it.
	stateBytes, err := os.ReadFile(filepath.Join(tmpDir, "state.json"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read staged state: "+err.Error())
		return
	}
	var probe State
	if err := json.Unmarshal(stateBytes, &probe); err != nil {
		writeError(w, http.StatusBadRequest, "staged state.json is not valid Jetty state: "+err.Error())
		return
	}

	// Commit: move staged state.json into place, replace compose dir.
	if err := os.Rename(filepath.Join(tmpDir, "state.json"), filepath.Join(a.dataDir, "state.json")); err != nil {
		writeError(w, http.StatusInternalServerError, "commit state.json: "+err.Error())
		return
	}

	stagedCompose := filepath.Join(tmpDir, "compose")
	if _, err := os.Stat(stagedCompose); err == nil {
		// Replace compose dir atomically: rename current to .old, move
		// staged into place, drop .old. We do best-effort rather than
		// atomic-or-bust because restore is rare and a partial state is
		// recoverable by re-running restore.
		oldCompose := a.composeDir + ".restore-old"
		os.RemoveAll(oldCompose)
		_ = os.Rename(a.composeDir, oldCompose)
		if err := os.Rename(stagedCompose, a.composeDir); err != nil {
			// Roll back
			os.Rename(oldCompose, a.composeDir)
			writeError(w, http.StatusInternalServerError, "commit compose dir: "+err.Error())
			return
		}
		os.RemoveAll(oldCompose)
	}

	writeJSON(w, map[string]string{
		"status": "restored",
		"note":   "restart the agent to load the new state",
	})
}

// writeFileToTar adds a file from disk into the tar writer at archivePath.
// No-op if the file doesn't exist.
func writeFileToTar(tw *tar.Writer, srcPath, archivePath string) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("expected file, got directory: %s", srcPath)
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()
	hdr := &tar.Header{
		Name:    archivePath,
		Mode:    int64(info.Mode().Perm()),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}

// writeDirToTar walks srcDir and writes every file under it into the tar
// writer, prefixing each entry with archivePrefix.
func writeDirToTar(tw *tar.Writer, srcDir, archivePrefix string) error {
	return filepath.Walk(srcDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, p)
		if err != nil {
			return err
		}
		archivePath := archivePrefix
		if rel != "." {
			archivePath = filepath.Join(archivePrefix, rel)
		}

		if info.IsDir() {
			return tw.WriteHeader(&tar.Header{
				Name:     archivePath + "/",
				Mode:     int64(info.Mode().Perm()),
				Typeflag: tar.TypeDir,
				ModTime:  info.ModTime(),
			})
		}
		if !info.Mode().IsRegular() {
			return nil // skip symlinks, devices, etc.
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := tw.WriteHeader(&tar.Header{
			Name:    archivePath,
			Mode:    int64(info.Mode().Perm()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}); err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		return err
	})
}
