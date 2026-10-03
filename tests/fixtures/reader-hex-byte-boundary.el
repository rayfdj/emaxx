(mapcar
 (lambda (escape)
   (mapcar
    (lambda (prefix)
      (let ((text (read (concat "\"" prefix escape "\""))))
        (list (append text nil) (string-bytes text) (multibyte-string-p text))))
    (list "" (string 955))))
 '("\\x7f" "\\x80" "\\x080" "\\xff" "\\x0ff" "\\x100"
   "\\xd800" "\\x110000" "\\x3ffeff" "\\x3fff00" "\\x3fff7f"
   "\\x3fff80" "\\x3fffff"))
