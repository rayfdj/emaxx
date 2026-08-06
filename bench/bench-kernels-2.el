;;; bench2.el --- broader emaxx vs GNU benchmark kernels -*- lexical-binding: t; -*-

(defun bench2-float (n)
  "Float arithmetic: accumulate a product/sum chain."
  (let ((acc 0.0) (x 1.0))
    (dotimes (_ n acc)
      (setq x (* x 1.0000001))
      (setq acc (+ acc (* x 0.5) -0.25)))))

(defun bench2-vector (n)
  "Vector aset/aref churn over a fixed-size vector."
  (let ((v (make-vector 256 0)) (sum 0))
    (dotimes (i n sum)
      (aset v (% i 256) i)
      (setq sum (+ sum (aref v (% (* i 7) 256)))))))

(defun bench2-hash (n)
  "Hash table puthash/gethash mix."
  (let ((h (make-hash-table :test 'eq)) (sum 0))
    (dotimes (i n sum)
      (puthash (% i 512) i h)
      (setq sum (+ sum (or (gethash (% (* i 3) 512) h) 0))))))

(defun bench2-mapcar (n)
  "Closure-heavy mapcar over a medium list, repeated."
  (let ((l (number-sequence 1 100)) (acc 0))
    (dotimes (_ n acc)
      (setq acc (+ acc (apply #'+ (mapcar (lambda (x) (* x 2)) l)))))))

(defun bench2-sort (n)
  "Sort a shuffled list of integers with #'<, repeated."
  (let ((seed 7) (out nil))
    (dotimes (_ n out)
      (let ((l nil))
        (dotimes (i 300)
          (setq seed (% (+ (* seed 1103515245) 12345) 2147483647))
          (push (% seed 10000) l))
        (setq out (sort l #'<))))))

(defun bench2-regex (n)
  "string-match over a moderately long string, repeated."
  (let ((s (concat (make-string 200 ?x) "needle-42 " (make-string 100 ?y)))
        (hits 0))
    (dotimes (_ n hits)
      (when (string-match "needle-\\([0-9]+\\)" s)
        (setq hits (+ hits (string-to-number (match-string 1 s))))))))

(defun bench2-buffer (n)
  "Buffer editing: insert lines, walk, and slice text."
  (let ((total 0))
    (dotimes (_ n total)
      (with-temp-buffer
        (dotimes (i 200)
          (insert "line " (number-to-string i) "\n"))
        (goto-char (point-min))
        (while (search-forward "line" nil t)
          (setq total (+ total 1)))
        (setq total (+ total (length (buffer-substring (point-min) (point-max)))))))))

(defun bench2-format (n)
  "format calls with mixed specs."
  (let ((acc 0))
    (dotimes (i n acc)
      (setq acc (+ acc (length (format "%d-%s-%04x" i "tag" (* i 3))))))))

(defun bench2-plist (n)
  "plist-get over a medium property list."
  (let ((pl '(:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8)) (sum 0))
    (dotimes (i n sum)
      (setq sum (+ sum (or (plist-get pl (nth (% i 8) '(:a :b :c :d :e :f :g :h))) 0))))))

(provide 'bench2)
