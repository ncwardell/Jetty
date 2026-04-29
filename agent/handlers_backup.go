package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
// @Description Returns a tar.gz containing state.json, the compose directory, and the WARP state directory. Sufficient to reconstruct this node's view of the cluster on a fresh data dir given the same JETTY_SECRET.
// @Tags backup
// @Produce application/gzip
// @Success 200 {file} file "tar.gz archive"
// @Router /backup [get]
func (a *Agent) apiBackup(w http.ResponseWriter, r *http.Request) {
	// Backup contains state.json which now carries AdminKey,
	// EncryptionKey, SelfAPIKey, and every peer's APIKey. Hand-out to
	// a peer key would let that peer recover admin credentials. Admin
	// only.
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required for backup", http.StatusUnauthorized)
		return
	}

	// Persist any pending state before we read it back from disk.
	a.saveState()

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(
		`attachment; filename="jetty-backup-%s.tar.gz"`, time.Now().UTC().Format("20060102-150405")))

	gz := gzip.NewWriter(w)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	// state.json
	if err := writeFileToTar(tw, filepath.Join(a.dataDir, "state.json"), "state.json"); err != nil {
		log.Printf("Backup: failed to add state.json: %v", err)
		// Can't change response code now; just log and continue.
	}

	// hwid (so the restored node keeps its identity if restoring on the
	// same machine; if the operator wants a fresh identity they can delete
	// it manually after restore).
	if err := writeFileToTar(tw, filepath.Join(a.dataDir, "hwid"), "hwid"); err != nil {
		log.Printf("Backup: failed to add hwid: %v", err)
	}

	// compose/ directory tree (workload compose files + override files)
	if err := writeDirToTar(tw, a.composeDir, "compose"); err != nil {
		log.Printf("Backup: failed to add compose dir: %v", err)
	}

	// warp/ (WARP connector registration so the restored node doesn't need
	// to re-register with Cloudflare on first start). Best-effort - if WARP
	// isn't configured, just skip.
	warpDir := filepath.Join(a.dataDir, "warp")
	if _, err := os.Stat(warpDir); err == nil {
		if err := writeDirToTar(tw, warpDir, "warp"); err != nil {
			log.Printf("Backup: failed to add warp dir: %v", err)
		}
	}
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
		http.Error(w, "unauthorized: admin key required for restore", http.StatusUnauthorized)
		return
	}

	defer r.Body.Close()

	// Stage to a buffer first so a partial/broken upload doesn't half-write
	// our state. For typical clusters the backup is small (KBs to a few MB),
	// so buffering in memory is fine. Cap at 64 MB to avoid OOM from a
	// malicious uploader.
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, io.LimitReader(r.Body, 64<<20)); err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	gz, err := gzip.NewReader(buf)
	if err != nil {
		http.Error(w, "decompress: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer gz.Close()
	tr := tar.NewReader(gz)

	// First pass: validate that this looks like a Jetty backup before
	// touching disk. Specifically, expect state.json at the archive root.
	tmpDir, err := os.MkdirTemp(a.dataDir, "restore-*")
	if err != nil {
		http.Error(w, "mkdir temp: "+err.Error(), http.StatusInternalServerError)
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
			http.Error(w, "tar read: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Refuse path-traversal attempts. Tar entries should all be
		// relative paths under the archive root.
		clean := filepath.Clean(hdr.Name)
		if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
			http.Error(w, "archive contains unsafe path: "+hdr.Name, http.StatusBadRequest)
			return
		}
		target := filepath.Join(tmpDir, clean)
		// Defense in depth: ensure target is under tmpDir.
		if !strings.HasPrefix(target, tmpDir+string(os.PathSeparator)) && target != tmpDir {
			http.Error(w, "archive escapes target dir: "+hdr.Name, http.StatusBadRequest)
			return
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0700); err != nil {
				http.Error(w, "mkdir: "+err.Error(), http.StatusInternalServerError)
				return
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				http.Error(w, "mkdir parent: "+err.Error(), http.StatusInternalServerError)
				return
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if err != nil {
				http.Error(w, "open: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				http.Error(w, "write: "+err.Error(), http.StatusInternalServerError)
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
		http.Error(w, "archive does not contain state.json - not a Jetty backup?", http.StatusBadRequest)
		return
	}

	// Validate that state.json parses before we commit it.
	stateBytes, err := os.ReadFile(filepath.Join(tmpDir, "state.json"))
	if err != nil {
		http.Error(w, "read staged state: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var probe State
	if err := json.Unmarshal(stateBytes, &probe); err != nil {
		http.Error(w, "staged state.json is not valid Jetty state: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Commit: move staged state.json into place, replace compose dir.
	if err := os.Rename(filepath.Join(tmpDir, "state.json"), filepath.Join(a.dataDir, "state.json")); err != nil {
		http.Error(w, "commit state.json: "+err.Error(), http.StatusInternalServerError)
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
			http.Error(w, "commit compose dir: "+err.Error(), http.StatusInternalServerError)
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
