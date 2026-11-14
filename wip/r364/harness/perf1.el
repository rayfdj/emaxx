;; MODE=stdout|string|function|small emacs -Q --batch -l perf1.el > /dev/null
(let* ((mode (getenv "MODE")) (big (make-list 2000 '(a "str" 12 3.5 sym [v 1])))
       (t0 (float-time)))
  (cond
   ((equal mode "stdout") (dotimes (_ 60) (prin1 big)))
   ((equal mode "string") (dotimes (_ 60) (prin1-to-string big)))
   ((equal mode "function") (let ((n 0)) (dotimes (_ 12) (prin1 big (lambda (_c) (setq n (1+ n)))))))
   ((equal mode "small") (dotimes (_ 300000) (prin1-to-string '(a 1 "s")))))
  (message "%s %.3f" mode (- (float-time) t0)))
