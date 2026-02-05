//go:build windows

package fileutil

import (
	"log/slog"
	"os"

	"golang.org/x/sys/windows"
)

// isOwnerOnly returns true if the permission mode grants nothing to group or other.
func isOwnerOnly(perm os.FileMode) bool {
	return perm&0077 == 0
}

// restrictToCurrentUser sets a DACL on path that grants GENERIC_ALL only to
// the current user and blocks inherited ACEs. If any Windows API call fails,
// a warning is logged and the error is returned, but callers may choose to
// treat this as non-fatal (the file was already created with the requested
// Unix mode, which is the best-effort default on Windows).
func restrictToCurrentUser(path string) error {
	token := windows.GetCurrentProcessToken()

	user, err := token.GetTokenUser()
	if err != nil {
		slog.Warn("fileutil: cannot get current user SID, skipping DACL", "path", path, "err", err)
		return nil
	}

	trustee := windows.TrusteeValueFromSID(user.User.Sid)

	ea := []windows.EXPLICIT_ACCESS{
		{
			AccessPermissions: windows.GENERIC_ALL,
			AccessMode:        windows.SET_ACCESS,
			Inheritance:       windows.NO_INHERITANCE,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_USER,
				TrusteeValue: trustee,
			},
		},
	}

	acl, err := windows.ACLFromEntries(ea, nil)
	if err != nil {
		slog.Warn("fileutil: cannot build ACL, skipping DACL", "path", path, "err", err)
		return nil
	}

	secInfo := windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
	err = windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.SECURITY_INFORMATION(secInfo),
		nil,  // owner SID (unchanged)
		nil,  // group SID (unchanged)
		acl,  // DACL
		nil,  // SACL (unchanged)
	)
	if err != nil {
		slog.Warn("fileutil: cannot set DACL", "path", path, "err", err)
		return nil
	}
	return nil
}

// SecureWriteFile writes data to the named file, creating it if necessary.
// For owner-only modes, a DACL restricting access to the current user is applied.
func SecureWriteFile(path string, data []byte, perm os.FileMode) error {
	if err := os.WriteFile(path, data, perm); err != nil {
		return err
	}
	if isOwnerOnly(perm) {
		restrictToCurrentUser(path)
	}
	return nil
}

// SecureMkdirAll creates a directory path and all parents that do not yet exist.
// For owner-only modes, a DACL restricting access to the current user is applied
// to the final directory.
func SecureMkdirAll(path string, perm os.FileMode) error {
	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}
	if isOwnerOnly(perm) {
		restrictToCurrentUser(path)
	}
	return nil
}

// SecureChmod changes the mode of the named file.
// For owner-only modes, a DACL restricting access to the current user is applied.
func SecureChmod(path string, perm os.FileMode) error {
	if err := os.Chmod(path, perm); err != nil {
		return err
	}
	if isOwnerOnly(perm) {
		restrictToCurrentUser(path)
	}
	return nil
}

// SecureOpenFile opens the named file with specified flag and permissions.
// For owner-only modes on newly created files, a DACL restricting access to
// the current user is applied.
func SecureOpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if isOwnerOnly(perm) && (flag&os.O_CREATE != 0) {
		restrictToCurrentUser(path)
	}
	return f, nil
}
