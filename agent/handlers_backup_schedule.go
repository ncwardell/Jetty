package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// =============================================================================
// HTTP Handlers - scheduled backups
// =============================================================================
//
// GET    /api/backup/schedule  - read the current schedule
// POST   /api/backup/schedule  - create or update
// DELETE /api/backup/schedule  - disable
//
// All admin-only. The schedule is gossiped via state.json so a single
// configuration change propagates to every node, but only the
// lowest-HWID healthy node actually performs the backup at each
// interval - same election rule failover uses, which guarantees
// exactly-one writer without coordination.
//
// Backups land at $dataDir/backups/jetty-backup-<UTCtimestamp>.tar.gz
// (or .enc when a passphrase is set on the schedule). Retention,
// when > 0, keeps the N most-recent backups in that directory and
// deletes older ones.

const backupScheduleMinInterval = 5 // minutes

// apiGetBackupSchedule godoc
// @Summary Read the scheduled-backup config
// @Description Returns the cluster's current backup schedule (or null if disabled). Admin only.
// @Tags backup
// @Produce json
// @Success 200 {object} BackupSchedule
// @Router /backup/schedule [get]
func (a *Agent) apiGetBackupSchedule(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}
	a.stateMu.RLock()
	sched := a.state.BackupSchedule
	a.stateMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if sched == nil {
		_, _ = w.Write([]byte(`null`))
		return
	}
	// Redact the passphrase - present its existence, not its value.
	out := *sched
	if out.Passphrase != "" {
		out.Passphrase = "<set>"
	}
	json.NewEncoder(w).Encode(&out)
}

// apiSetBackupSchedule godoc
// @Summary Create or update the scheduled-backup config
// @Description Admin-only. Body fields: interval_minutes (>=5), retention (0=unlimited), passphrase (optional, JETTY-ENC-V1 wraps every backup if set).
// @Tags backup
// @Accept json
// @Produce json
// @Param request body BackupSchedule true "Schedule"
// @Success 200 {object} BackupSchedule
// @Failure 400 {object} ErrorResponse
// @Router /backup/schedule [post]
func (a *Agent) apiSetBackupSchedule(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}
	var req BackupSchedule
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.IntervalMinutes < backupScheduleMinInterval {
		http.Error(w, fmt.Sprintf("interval_minutes must be >= %d", backupScheduleMinInterval), http.StatusBadRequest)
		return
	}
	if req.Retention < 0 {
		http.Error(w, "retention must be >= 0", http.StatusBadRequest)
		return
	}
	req.UpdatedAt = time.Now().UTC()
	// Preserve last-run telemetry across schedule edits so the
	// dashboard can keep showing "last backup at X" while the
	// operator tweaks interval/retention.
	a.stateMu.Lock()
	if old := a.state.BackupSchedule; old != nil {
		req.LastRunAt = old.LastRunAt
		req.LastStatus = old.LastStatus
		req.LastBackupPath = old.LastBackupPath
	}
	a.state.BackupSchedule = &req
	a.stateMu.Unlock()
	a.saveState()
	a.broadcastState()

	out := req
	if out.Passphrase != "" {
		out.Passphrase = "<set>"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&out)
	log.Printf("Backup schedule updated: every %dm, retention=%d, encrypted=%v",
		req.IntervalMinutes, req.Retention, req.Passphrase != "")
}

// apiDeleteBackupSchedule godoc
// @Summary Disable scheduled backups
// @Description Removes the cluster's scheduled-backup config. Existing backup files on disk are not deleted. Admin only.
// @Tags backup
// @Success 204
// @Router /backup/schedule [delete]
func (a *Agent) apiDeleteBackupSchedule(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}
	a.stateMu.Lock()
	a.state.BackupSchedule = nil
	a.stateMu.Unlock()
	a.saveState()
	a.broadcastState()
	w.WriteHeader(http.StatusNoContent)
	log.Printf("Backup schedule disabled")
}

// shouldRunScheduledBackup decides whether THIS node should write
// the next backup. Same election rule as failover: only the
// lowest-HWID healthy node runs; everyone else skips. Avoids needing
// a leader election protocol.
func (a *Agent) shouldRunScheduledBackup() bool {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	candidates := []string{a.hwid}
	for _, p := range a.state.Peers {
		if p.Healthy {
			candidates = append(candidates, p.ID)
		}
	}
	sort.Strings(candidates)
	return len(candidates) > 0 && candidates[0] == a.hwid
}

// runScheduledBackup writes a single backup using the current
// schedule's settings. Called from the loop when the interval has
// elapsed. Updates LastRunAt / LastStatus / LastBackupPath in state.
func (a *Agent) runScheduledBackup() {
	a.stateMu.RLock()
	sched := a.state.BackupSchedule
	a.stateMu.RUnlock()
	if sched == nil {
		return
	}

	backupsDir := filepath.Join(a.dataDir, "backups")
	if err := os.MkdirAll(backupsDir, 0700); err != nil {
		a.recordBackupRunResult("error: mkdir: " + err.Error(), "")
		return
	}

	stamp := time.Now().UTC().Format("20060102-150405")
	suffix := ".tar.gz"
	if sched.Passphrase != "" {
		suffix = ".tar.gz.enc"
	}
	path := filepath.Join(backupsDir, "jetty-backup-"+stamp+suffix)

	// Reuse writeBackup by handing it a httptest.ResponseRecorder
	// (we don't actually serve HTTP here - we just want the same
	// build-tar.gz + optionally-encrypt path). Then copy the bytes
	// to the on-disk file.
	rec := httptest.NewRecorder()
	a.writeBackup(rec, sched.Passphrase, filepath.Base(path))
	if rec.Code != 200 {
		a.recordBackupRunResult("error: build status "+fmt.Sprint(rec.Code), "")
		return
	}
	if err := os.WriteFile(path, rec.Body.Bytes(), 0600); err != nil {
		a.recordBackupRunResult("error: write: "+err.Error(), "")
		return
	}

	// Apply retention.
	if sched.Retention > 0 {
		if err := a.pruneBackups(backupsDir, sched.Retention); err != nil {
			log.Printf("Backup retention: %v", err)
		}
	}

	a.recordBackupRunResult("ok", path)
	log.Printf("Scheduled backup written: %s (%d bytes)", path, rec.Body.Len())
}

func (a *Agent) recordBackupRunResult(status, path string) {
	a.stateMu.Lock()
	if a.state.BackupSchedule != nil {
		a.state.BackupSchedule.LastRunAt = time.Now().UTC()
		a.state.BackupSchedule.LastStatus = status
		if path != "" {
			a.state.BackupSchedule.LastBackupPath = path
		}
	}
	a.stateMu.Unlock()
	a.saveState()
}

// pruneBackups keeps the N newest jetty-backup-* files, deletes the rest.
func (a *Agent) pruneBackups(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type fi struct {
		name string
		mod  time.Time
	}
	var files []fi
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(e.Name(), "jetty-backup-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fi{name: e.Name(), mod: info.ModTime()})
	}
	if len(files) <= keep {
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	for _, f := range files[keep:] {
		if err := os.Remove(filepath.Join(dir, f.name)); err != nil {
			log.Printf("retention: remove %s: %v", f.name, err)
		}
	}
	return nil
}

// scheduledBackupLoop is the background ticker. Runs once a minute,
// checks the schedule, and triggers a backup if the interval has
// elapsed since last_run.
func (a *Agent) scheduledBackupLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			a.maybeRunScheduledBackup()
		}
	}
}

func (a *Agent) maybeRunScheduledBackup() {
	a.stateMu.RLock()
	sched := a.state.BackupSchedule
	a.stateMu.RUnlock()
	if sched == nil || sched.IntervalMinutes <= 0 {
		return
	}
	if !a.shouldRunScheduledBackup() {
		return
	}
	if !sched.LastRunAt.IsZero() && time.Since(sched.LastRunAt) < time.Duration(sched.IntervalMinutes)*time.Minute {
		return
	}
	a.runScheduledBackup()
}
