#lang racket/base

;; The module provides parsing and serialization for MessagePack.

(require racket/contract/base)

(provide
  (contract-out
    ;; Check if a Racket value can be serialized to MessagePack.
    [msgpack-expr? (->* (any/c) (#:nil any/c) any)]
    ;; The value used to represent MessagePack nil.
    [msgpack-nil (parameter/c any/c)]
    ;; Write the given value serialized as MessagePack to the port.
    [msgpack-write (->* (any/c) (output-port? #:nil any/c) any)]
    ;; Create a byte string with the given value formatted as MessagePack.
    [msgpack-pack (->* (any/c) (#:nil any/c) bytes?)]
    ;; Read a MessagePack value from the port.
    [msgpack-read (->* () (input-port? #:nil any/c) (or/c msgpack-expr? eof-object?))]
    ;; Read a MessagePack value from the byte string.
    [msgpack-unpack (->* (bytes?) (#:nil any/c) msgpack-expr?)]))

(require "format-bytes.rkt")

;; -------------
;; Customization

(define msgpack-nil (make-parameter 'nil))

;; ---------
;; Predicate

(define (msgpack-expr? x #:nil [nil (msgpack-nil)])
  (let check ([x x])
    (or (and (exact-integer? x)
             (>= x (- (expt 2 63)))
             (<= x (sub1 (expt 2 64))))
        (double-flonum? x)
        (single-flonum? x)
        (boolean? x)
        ;; This check is not exhaustive as strings and byte strings taking more than 2^32 - 1
        ;; bytes return #t.
        (string? x)
        (bytes? x)
        (eq? x nil)
        (and (list? x) (andmap check x))
        (and (hash? x)
             (for/and ([(k v) (in-hash x)])
               (and (check k) (check v)))))))

;; ----------------
;; Writing to ports

(define (write-int-bytes n size-n signed? out)
  (write-bytes (integer->integer-bytes n size-n signed? #t) out))

(define (msgpack-error-write-int n)
  (raise-arguments-error 'msgpack-write-int
                         "expected n where: -(2^63) <= n < (2^64)"
                         "n" n))

(define (msgpack-write-int n out)
  (define len (integer-length n))
  (if (not (negative? n))
    (cond
      [(< len 8) (write-byte n out)]
      [(<= len 8) (write-byte format:uint8 out) (write-byte n out)]
      [(<= len 16) (write-byte format:uint16 out) (write-int-bytes n 2 #f out)]
      [(<= len 32) (write-byte format:uint32 out) (write-int-bytes n 4 #f out)]
      [(<= len 64) (write-byte format:uint64 out) (write-int-bytes n 8 #f out)]
      [else (msgpack-error-write-int n)])
    (cond
      [(< len 6) (write-byte (- 256 (abs n)) out)]
      [(< len 8) (write-byte format:int8 out) (write-byte (- 256 (abs n)) out)]
      [(< len 16) (write-byte format:int16 out) (write-int-bytes n 2 #t out)]
      [(< len 32) (write-byte format:int32 out) (write-int-bytes n 4 #t out)]
      [(< len 64) (write-byte format:int64 out) (write-int-bytes n 8 #t out)]
      [else (msgpack-error-write-int n)])))

(define (msgpack-write-string str out)
  (define len (string-utf-8-length str))
  (cond [(< len #x20) (write-byte (+ format:min-fixstr len) out) (write-string str out)]
        [(<= len #xFF) (write-byte format:str8 out) (write-byte len out) (write-string str out)]
        [(<= len #xFFff) (write-byte format:str16 out) (write-int-bytes len 2 #f out)
                         (write-string str out)]
        [(<= len #xFFffFFff) (write-byte format:str32 out) (write-int-bytes len 4 #f out)
                             (write-string str out)]
        [else (raise-arguments-error 'msgpack-write-string
                                     "string too long"
                                     "(string-utf-8-length str)" len)]))

(define (msgpack-write-bytes bstr out)
  (define len (bytes-length bstr))
  (cond [(<= len #xFF) (write-byte format:bin8 out) (write-byte len out) (write-bytes bstr out)]
        [(<= len #xFFff) (write-byte format:bin16 out) (write-int-bytes len 2 #f out)
                         (write-bytes bstr out)]
        [(<= len #xFFffFFff) (write-byte format:bin32 out) (write-int-bytes len 4 #f out)
                             (write-bytes bstr out)]
        [else (raise-arguments-error 'msgpack-write-bytes
                                     "bytestring too long"
                                     "(bytes-length bstr)" len)]))

(define (msgpack-write-array-head x out)
  (define len (length x))
  (cond
    [(<= len #xF) (write-byte (+ format:min-fixarray len) out)]
    [(<= len #xFFff) (write-byte format:array16 out) (write-int-bytes len 2 #f out)]
    [(<= len #xFFffFFff) (write-byte format:array32 out) (write-int-bytes len 4 #f out)]
    [else (raise-arguments-error 'msgpack-write-array-head "")]))

(define (msgpack-write-map-head x out)
  (define len (hash-count x))
  (cond
    [(<= len #xF) (write-byte (+ format:min-fixmap len) out)]
    [(<= len #xFFff) (write-byte format:map16 out) (write-int-bytes len 2 #f out)]
    [(<= len #xFFffFFff) (write-byte format:map32 out) (write-int-bytes len 4 #f out)]
    [else (raise-arguments-error 'msgpack-write-map-head "")]))

(define (msgpack-write x [out (current-output-port)] #:nil [nil (msgpack-nil)])
  (let recurse ([x x])
    (cond [(eq? x nil) (write-byte format:nil out)]
          [(boolean? x) (write-byte (if x format:true format:false) out)]
          [(exact-integer? x) (msgpack-write-int x out)]
          [(single-flonum? x) (write-byte format:float32 out)
                              (write-bytes (real->floating-point-bytes x 4 #t) out)]
          [(double-flonum? x) (write-byte format:float64 out)
                              (write-bytes (real->floating-point-bytes x 8 #t) out)]
          [(string? x) (msgpack-write-string x out)]
          [(bytes? x) (msgpack-write-bytes x out)]
          [(list? x) (msgpack-write-array-head x out) (for ([e x]) (recurse e))]
          [(hash? x) (msgpack-write-map-head x out) (for ([(k v) (in-hash x)]) (recurse k) (recurse v))]
          [else (raise-arguments-error 'write-msgpack
                                       "expected x where: (msgpack-expr? x)"
                                       "x" x)])))

(define (msgpack-pack x #:nil [nil (msgpack-nil)])
  (define ret (open-output-bytes))
  (msgpack-write x ret #:nil nil)
  (get-output-bytes ret))

;; ------------------
;; Reading from ports

(define (msgpack-read-int byte-count in [signed #f])
  (integer-bytes->integer (read-bytes byte-count in) signed #t))

(define (msgpack-read [in (current-input-port)] #:nil [nil (msgpack-nil)])
  (let ([fb (read-byte in)])
    (cond
      [(<= fb format:max-pos-fixint) fb]
      [(<= fb format:max-fixmap) (for/hash ([i (in-range (- fb format:min-fixmap))])
                                   (values (msgpack-read in #:nil nil)
                                           (msgpack-read in #:nil nil)))]
      [(<= fb format:max-fixarray) (for/list ([i (in-range (- fb format:min-fixarray))])
                                     (msgpack-read in #:nil nil))]
      [(<= fb format:max-fixstr) (bytes->string/utf-8 (read-bytes (- fb format:min-fixstr) in))]
      [(= fb format:nil) nil]
      [(= fb format:false) #f]
      [(= fb format:true) #t]
      [(= fb format:uint8) (read-byte in)]
      [(= fb format:uint16) (msgpack-read-int 2 in)]
      [(= fb format:uint32) (msgpack-read-int 4 in)]
      [(= fb format:uint64) (msgpack-read-int 8 in)]
      [(= fb format:int8) (let ([x (read-byte in)])
                            (if (>= x #x80)
                              (- (add1 (bitwise-and 255 (bitwise-not x))))
                              x))]
      [(= fb format:int16) (msgpack-read-int 2 in #t)]
      [(= fb format:int32) (msgpack-read-int 4 in #t)]
      [(= fb format:int64) (msgpack-read-int 8 in #t)]
      [(= fb format:float32) (floating-point-bytes->real (read-bytes 4 in) #t)]
      [(= fb format:float64) (floating-point-bytes->real (read-bytes 8 in) #t)]
      [(= fb format:str8) (bytes->string/utf-8 (read-bytes (read-byte in) in))]
      [(= fb format:str16) (bytes->string/utf-8 (read-bytes (msgpack-read-int 2 in) in))]
      [(= fb format:str32) (bytes->string/utf-8 (read-bytes (msgpack-read-int 4 in) in))]
      [(= fb format:bin8) (read-bytes (read-byte in) in)]
      [(= fb format:bin16) (read-bytes (msgpack-read-int 2 in) in)]
      [(= fb format:bin32) (read-bytes (msgpack-read-int 4 in) in)]
      [(= fb format:array16) (for/list ([i (in-range (msgpack-read-int 2 in))])
                                    (msgpack-read in #:nil nil))]
      [(= fb format:array32) (for/list ([i (in-range (msgpack-read-int 4 in))])
                                    (msgpack-read in #:nil nil))]
      [(= fb format:map16) (for/hash ([i (in-range (msgpack-read-int 2 in))])
                                  (values (msgpack-read in #:nil nil)
                                          (msgpack-read in #:nil nil)))]
      [(= fb format:map32) (for/hash ([i (in-range (msgpack-read-int 4 in))])
                                  (values (msgpack-read in #:nil nil)
                                          (msgpack-read in #:nil nil)))]
      [(and (>= fb format:min-neg-fixint) (<= fb format:max-neg-fixint))
       (- (add1 (bitwise-and 255 (bitwise-not fb))))]
      [(eof-object? fb) fb])))

(define (msgpack-unpack bstr #:nil [nil (msgpack-nil)])
  (define in (open-input-bytes bstr))
  (msgpack-read in #:nil nil))
