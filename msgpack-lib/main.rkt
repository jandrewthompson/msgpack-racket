#lang racket/base

;; The module provides parsing and serialization for MessagePack.

(require racket/contract/base
         racket/dict)

(define msgpack-nil (make-parameter 'nil))

(define msgpack-expr/c
  (flat-rec-contract expr
    (or/c (integer-in (- (expt 2 63)) (sub1 (expt 2 64)))
          single-flonum?
          double-flonum?
          boolean?
          ;; This check is not exhaustive as strings and byte strings taking more than 2^32 - 1
          ;; bytes return #t.
          string?
          bytes?
          (msgpack-nil)
          (vectorof expr #:flat? #t)
          (listof expr)
          (hash/c expr expr #:flat? #t))))

(provide
  (contract-out
    [msgpack-expr/c flat-contract?]
    ;; The value used to represent MessagePack nil.
    [msgpack-nil (parameter/c any/c)]
    ;; Write the given value serialized as MessagePack to the port.
    [msgpack-write (-> msgpack-expr/c output-port? any)]
    ;; Create a byte string with the given value formatted as MessagePack.
    [msgpack-pack (-> msgpack-expr/c bytes?)]
    ;; Read a MessagePack value from the port.
    [msgpack-read (-> input-port? (or/c msgpack-expr/c eof-object?))]
    ;; Read a MessagePack value from the byte string.
    [msgpack-unpack (-> bytes? msgpack-expr/c)]))

(require "format-bytes.rkt")

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

(define (msgpack-write-array-head len out)
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

(define (msgpack-write x out)
  (let recurse ([x x])
    (cond [(eq? x (msgpack-nil)) (write-byte format:nil out)]
          [(boolean? x) (write-byte (if x format:true format:false) out)]
          [(exact-integer? x) (msgpack-write-int x out)]
          [(single-flonum? x) (write-byte format:float32 out)
                              (write-bytes (real->floating-point-bytes x 4 #t) out)]
          [(double-flonum? x) (write-byte format:float64 out)
                              (write-bytes (real->floating-point-bytes x 8 #t) out)]
          [(string? x) (msgpack-write-string x out)]
          [(bytes? x) (msgpack-write-bytes x out)]
          [(vector? x) (msgpack-write-array-head (vector-length x) out) (for ([e x]) (recurse e))]
          [(list? x) (msgpack-write-array-head (length x) out) (for ([e x]) (recurse e))]
          [(hash? x) (msgpack-write-map-head x out) (for ([(k v) (in-hash x)]) (recurse k) (recurse v))]
          [else (raise-arguments-error 'write-msgpack
                                       "expected x where: (msgpack-expr? x)"
                                       "x" x)])))

(define (msgpack-pack x)
  (define ret (open-output-bytes))
  (msgpack-write x ret)
  (get-output-bytes ret))

;; ------------------
;; Reading from ports

(define (must-read-byte in)
  (define b (read-byte in))
  (if (eof-object? b)
    (error 'must-read-byte "unexpected eof")
    b))

(define (must-read-bytes amt in)
  (define bstr (read-bytes amt in))
  (if (= (bytes-length bstr) amt)
    bstr
    (error 'must-read-bytes "failed to read ~a bytes from ~a" amt in)))

(define (msgpack-must-read in)
  (define v (msgpack-read in))
  (if (eof-object? v)
    (error 'msgpack-must-read "unexpected eof")
    v))

(define-syntax-rule (read-int-data len in)
  (integer-bytes->integer (must-read-bytes len in) #t #t))

(define-syntax-rule (read-uint-data len in)
  (integer-bytes->integer (must-read-bytes len in) #f #t))

(define-syntax-rule (read-array-data len in)
  (for/vector #:length len ([_ (in-naturals)]) (msgpack-must-read in)))

(define-syntax-rule (read-map-data len in)
  (for/hash ([_ (in-range len)]) (values (msgpack-must-read in) (msgpack-must-read in))))

(define-syntax-rule (read-str-data len in)
  (bytes->string/utf-8 (must-read-bytes len in)))

(define (msgpack-read in)
  (define fb (read-byte in))
  (cond
    [(<= fb format:max-pos-fixint) fb]
    [(<= fb format:max-fixmap) (read-map-data (- fb format:min-fixmap) in)]
    [(<= fb format:max-fixarray) (read-array-data (- fb format:min-fixarray) in)]
    [(<= fb format:max-fixstr) (read-str-data (- fb format:min-fixstr) in)]
    [(= fb format:nil) (msgpack-nil)]
    [(= fb format:false) #f]
    [(= fb format:true) #t]
    [(= fb format:uint8) (must-read-byte in)]
    [(= fb format:uint16) (read-uint-data 2 in)]
    [(= fb format:uint32) (read-uint-data 4 in)]
    [(= fb format:uint64) (read-uint-data 8 in)]
    [(= fb format:int8) (let ([x (must-read-byte in)])
                          (if (>= x #x80)
                            (- (add1 (bitwise-and 255 (bitwise-not x))))
                            x))]
    [(= fb format:int16) (read-int-data 2 in)]
    [(= fb format:int32) (read-int-data 4 in)]
    [(= fb format:int64) (read-int-data 8 in)]
    [(= fb format:float32) (floating-point-bytes->real (must-read-bytes 4 in) #t)]
    [(= fb format:float64) (floating-point-bytes->real (must-read-bytes 8 in) #t)]
    [(= fb format:str8) (read-str-data (must-read-byte in) in)]
    [(= fb format:str16) (read-str-data (read-uint-data 2 in) in)]
    [(= fb format:str32) (read-str-data (read-uint-data 4 in) in)]
    [(= fb format:bin8) (must-read-bytes (must-read-byte in) in)]
    [(= fb format:bin16) (must-read-bytes (read-uint-data 2 in) in)]
    [(= fb format:bin32) (must-read-bytes (read-uint-data 4 in) in)]
    [(= fb format:array16) (read-array-data (read-uint-data 2 in) in)]
    [(= fb format:array32) (read-array-data (read-uint-data 4 in) in)]
    [(= fb format:map16) (read-map-data (read-uint-data 2 in) in)]
    [(= fb format:map32) (read-map-data (read-uint-data 4 in) in)]
    [(and (>= fb format:min-neg-fixint) (<= fb format:max-neg-fixint))
     (- (add1 (bitwise-and 255 (bitwise-not fb))))]
    [(eof-object? fb) fb]))

(define (msgpack-unpack bstr)
  (define in (open-input-bytes bstr))
  (msgpack-read in))
