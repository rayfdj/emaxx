;;; eat_process_gate.el --- Eat 0.9.4 real-process gate -*- lexical-binding: t; -*-

;; This script is intentionally editor-neutral.  The Python package gate runs
;; this exact file against GNU Emacs and Emaxx with separate clean package
;; roots, then requires their emitted records to be identical.

(setq user-emacs-directory
      (file-name-as-directory
       (or (getenv "EAT_GATE_ROOT")
           (error "EAT_GATE_ROOT is required")))
      package-user-dir (expand-file-name "packages" user-emacs-directory)
      eat-kill-buffer-on-exit nil
      eat-enable-shell-prompt-annotation nil
      eat-minimum-latency 0
      eat-maximum-latency 0
      eat-term-scrollback-size nil)

(require 'package)
(package-initialize)
(require 'eat)

(let ((origin (locate-library "eat")))
  (unless (and origin
               (file-in-directory-p origin package-user-dir)
               (string-match-p "/eat-0\\.9\\.4/eat\\.elc\\'" origin))
    (error "Eat did not resolve to installed 0.9.4 bytecode: %S" origin)))

(defun eat-gate--emit (key value)
  (princ (format "EAT_GATE\t%s\t%S\n" key value)))

(defun eat-gate--has (buffer text)
  (with-current-buffer buffer
    (save-excursion
      (goto-char (point-min))
      (search-forward text nil t))))

(defun eat-gate--wait (process predicate label &optional seconds)
  (let ((deadline (+ (float-time) (or seconds 20.0))))
    (while (and (not (funcall predicate))
                (< (float-time) deadline))
      (accept-process-output process 0.05))
    (unless (funcall predicate)
      (error "Timed out waiting for %s (status %S)" label
             (process-status process)))))

(defun eat-gate--deterministic-process ()
  (let* ((name (format "eat-deterministic-%d" (emacs-pid)))
         (script
          (concat
           "printf '\\033[2J\\033[HREADY\\n'\n"
           "IFS= read -r trigger\n"
           "printf 'SIZE:'; stty size\n"
           "printf '\\033[31mRED\\033[0m\\n'\n"
           "printf '\\033[?1049hALT-ONLY\\n\\033[?1049l'\n"
           "printf '\\033[4;7HCURSOR'\n"
           "IFS= read -r trigger\n"
           "printf '\\033[6;1HLINE:%s\\n' \"$trigger\"\n"
           "i=0; while [ $i -lt 200 ]; do "
           "printf 'ROW:%03d\\n' $i; i=$((i+1)); done\n"
           "printf 'WAITEOF\\n'\n"
           "IFS= read -r ignored\n"
           "printf 'EOF:%s\\n' \"$?\"\n"
           "exit 7\n"))
         (buffer (eat-make name "/bin/sh" nil "-c" script))
         process cursor-shape pre-exit exit-status exit-kind)
    (with-current-buffer buffer
      (setq process (get-buffer-process buffer))
      (add-hook 'eat-exit-hook
                (lambda (proc)
                  (setq exit-status (process-exit-status proc)
                        exit-kind (process-status proc)))
                nil t))
    (eat-gate--wait
     process (lambda () (eat-gate--has buffer "READY")) "READY")
    (with-current-buffer buffer
      (eat-term-resize eat-terminal 100 40)
      (set-process-window-size process 40 100)
      (eat-term-send-string eat-terminal "size-now\n"))
    (eat-gate--wait
     process (lambda () (eat-gate--has buffer "CURSOR")) "CURSOR")
    (with-current-buffer buffer
      (let ((cursor (eat-term-display-cursor eat-terminal)))
        (setq cursor-shape
              (list (line-number-at-pos cursor)
                    (save-excursion
                      (goto-char cursor)
                      (current-column)))))
      (eat-term-send-string eat-terminal "payload\n"))
    (eat-gate--wait
     process (lambda () (eat-gate--has buffer "WAITEOF")) "WAITEOF")
    (with-current-buffer buffer
      (let ((red (save-excursion
                   (goto-char (point-min))
                   (search-forward "RED" nil t))))
        (setq pre-exit
              (list
               (eat-term-size eat-terminal)
               cursor-shape
               (and red (get-text-property (1- red) 'face))
               (save-excursion
                 (goto-char (point-min))
                 (how-many "^ROW:" (point-min) (point-max)))
               (not (eat-gate--has buffer "ALT-ONLY"))
               (and (eat-gate--has buffer "SIZE:40 100") t)
               (and (eat-gate--has buffer "LINE:payload") t))))
      (eat-term-input-event eat-terminal 1 ?\C-d))
    (eat-gate--wait
     process
     (lambda () (with-current-buffer buffer (null eat-terminal)))
     "deterministic process exit")
    (with-current-buffer buffer
      (let ((result
             (list
              pre-exit
              (and (eat-gate--has buffer "EOF:1") t)
              exit-kind
              exit-status
              (null (get-buffer-process buffer))
              (null eat-terminal)
              (not buffer-read-only))))
        (kill-buffer buffer)
        result))))

(defun eat-gate--signal-process ()
  (let* ((name (format "eat-signal-%d" (emacs-pid)))
         (buffer
          (eat-make name "/bin/sh" nil "-c"
                    "printf 'WAITINT\\n'; exec /bin/sleep 30"))
         process exit-kind exit-status)
    (with-current-buffer buffer
      (setq process (get-buffer-process buffer))
      (add-hook 'eat-exit-hook
                (lambda (proc)
                  (setq exit-kind (process-status proc)
                        exit-status (process-exit-status proc)))
                nil t))
    (eat-gate--wait
     process (lambda () (eat-gate--has buffer "WAITINT")) "WAITINT")
    (with-current-buffer buffer
      (eat-term-input-event eat-terminal 1 ?\C-c))
    (eat-gate--wait
     process
     (lambda () (with-current-buffer buffer (null eat-terminal)))
     "SIGINT process exit")
    (with-current-buffer buffer
      (let ((result
             (list exit-kind exit-status
                   (and (eat-gate--has buffer "WAITINT") t)
                   (null (get-buffer-process buffer))
                   (null eat-terminal)
                   (not buffer-read-only))))
        (kill-buffer buffer)
        result))))

(defun eat-gate--interactive-shell ()
  (let* ((process-environment
          (cons "PS1=EAT-CERT> "
                (cons "ENV=/dev/null" process-environment)))
         (name (format "eat-shell-%d" (emacs-pid)))
         (buffer (eat-make name "/bin/sh" nil "-i"))
         process exit-kind exit-status face)
    (with-current-buffer buffer
      (setq process (get-buffer-process buffer))
      (add-hook 'eat-exit-hook
                (lambda (proc)
                  (setq exit-kind (process-status proc)
                        exit-status (process-exit-status proc)))
                nil t))
    ;; Host shells may replace inherited PS1.  Exercise startup traffic but
    ;; assert evaluated output rather than a host-specific prompt spelling.
    (accept-process-output process 1.0)
    (with-current-buffer buffer
      (eat-term-send-string
       eat-terminal
       (concat "printf 'SHELL-OK:%s\\n' \"$((6*7))\"; "
               "printf '\\033[35mMAGENTA\\033[0m\\n'\n")))
    (eat-gate--wait
     process
     (lambda () (eat-gate--has buffer "SHELL-OK:42"))
     "interactive shell output")
    (with-current-buffer buffer
      (save-excursion
        (goto-char (point-min))
        (while (search-forward "MAGENTA" nil t)
          (when-let* ((candidate
                       (get-text-property (1- (point)) 'face)))
            (setq face candidate))))
      (eat-term-send-string eat-terminal "exit 3\n"))
    (eat-gate--wait
     process
     (lambda () (with-current-buffer buffer (null eat-terminal)))
     "interactive shell exit")
    (with-current-buffer buffer
      (let ((result
             (list
              (and (eat-gate--has buffer "SHELL-OK:42") t)
              face exit-kind exit-status
              (null (get-buffer-process buffer))
              (null eat-terminal)
              (not buffer-read-only))))
        (kill-buffer buffer)
        result))))

(condition-case eat-gate-error
    (progn
      (eat-gate--emit "deterministic" (eat-gate--deterministic-process))
      (eat-gate--emit "signal" (eat-gate--signal-process))
      (eat-gate--emit "shell" (eat-gate--interactive-shell)))
  (error
   (princ (format "EAT_GATE_ERROR\t%S\n" eat-gate-error))
   (kill-emacs 1)))

;;; eat_process_gate.el ends here
