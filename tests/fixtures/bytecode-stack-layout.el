(let ((depths '(1 262143 262144 393216 524278 524279 524280 524281 524284 524288))
      (pairs '((1 524275) (1 524276) (262138 262138) (262139 262138)
               (393216 131060) (393216 131061)))
      (answers nil))
  ;; A GNU stack has 524288 words, including a four-word dummy footer
  ;; and one four-word footer for every active closure. Arguments are
  ;; already included in each closure's declared depth.
  (dolist (depth depths)
    (push (list 'single depth
                (condition-case condition
                    (funcall (make-byte-code 0 (unibyte-string 192 135)
                                             [capacity-value] depth))
                  (error condition))) answers)
    (push (list 'argument depth
                (condition-case condition
                    (funcall (make-byte-code 257 (unibyte-string 135)
                                             [] depth) 'argument-value)
                  (error condition))) answers))
  (dolist (pair pairs)
    (let* ((inner (make-byte-code 0 (unibyte-string 192 135)
                                  [nested-value] (cadr pair)))
           (outer (make-byte-code 0 (unibyte-string 192 32 135)
                                  (vector inner) (car pair))))
      (push (list 'nested pair
                  (condition-case condition (funcall outer)
                    (error condition))) answers)))
  ;; A signaled callee must release its reservation before the caller's
  ;; handler can enter another large frame.
  (let ((too-large (make-byte-code 0 (unibyte-string 192 135) [unused] 524288))
        (usable (make-byte-code 0 (unibyte-string 192 135) [after-overflow] 393216)))
    (push (list 'recovery
                (condition-case condition (funcall too-large)
                  (error (list (car condition) (funcall usable))))) answers))
  (nreverse answers))
