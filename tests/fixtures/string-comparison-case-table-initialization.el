(list
 (list (compare-strings "\u038c\u03c3\u03bf\u03c2" nil nil "\u038c\u03a3\u039f\u03a3" nil nil t)
       (compare-strings "\u1e9e" nil nil "\u00df" nil nil t))
 (with-temp-buffer
   (set-case-table (make-char-table 'case-table nil))
   (list (compare-strings "\u038c\u03c3\u03bf\u03c2" nil nil "\u038c\u03a3\u039f\u03a3" nil nil t)
         (compare-strings "\u1e9e" nil nil "\u00df" nil nil t))))
