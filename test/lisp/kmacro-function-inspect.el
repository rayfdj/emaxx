;;; kmacro-function-inspect.el --- scratch kmacro function inspection -*- lexical-binding: t; -*-

(require 'kmacro)
(require 'ert)

(ert-deftest emaxx-kmacro-inspect-end-kbd-macro-function ()
  (ert-fail
   (format "symbol-function=%S type=%S fboundp=%S"
           (symbol-function 'end-kbd-macro)
           (type-of (symbol-function 'end-kbd-macro))
           (fboundp 'end-kbd-macro))))

