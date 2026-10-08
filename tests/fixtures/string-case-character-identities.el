(let ((codes '(?e ?E #xe9 #xc9 #x3fffe9 #x3fffc9 #xe0e9 #xe0c9
              #x110000 #x3fffff #x3c2 #x3a3 #xd800 #x1e9e #xdf))
      (result nil))
  (dolist (multibyte '(nil t))
    (with-temp-buffer
      (set-buffer-multibyte multibyte)
      (dolist (code codes)
        (push (list multibyte code (upcase code) (downcase code)
                    (aref (char-table-extra-slot (current-case-table) 0) code)
                    (aref (current-case-table) code))
              result))))
  (nreverse result))
