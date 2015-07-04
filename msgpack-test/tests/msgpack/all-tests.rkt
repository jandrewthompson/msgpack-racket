#lang racket/base

(require (for-syntax racket/base)
         racket/list
         rackunit
         msgpack)

;; Check whether an atom can be packed into the expected byte string and whether that byte
;; string can be unpacked to produce the original atom.
(define-syntax check-msgpack
  (syntax-rules ()
    ;; Test for packing only in cases where unpacking the packed value would produce some
    ;; different value. For example we always produce double precision floats, even when
    ;; reading single precision floats.
    [(_ #:pack v bstr) (check-equal? (msgpack-pack v) bstr)]
    ;; Test for unpacking only in cases where the packer would choose a different but
    ;; equivalent format for serializing the data. This happens a lot for integers, where
    ;; the library always prefers to pack the value into the smallest possible byte string,
    ;; with a preference for unsigned integers over signed ones.
    [(_ #:unpack v bstr)  (check-equal? (msgpack-unpack bstr) v)]
    ;; In all other cases test conversion both ways, to ensure that the packer and the
    ;; unpacker agrees on how a value should be represented.
    [(_ v bstr) (begin (check-msgpack #:pack v bstr) (check-msgpack #:unpack v bstr))]))

(define-syntax-rule (check-atoms [x ...] ...)
  (begin (check-msgpack x ...) ...))

(test-begin
  (check-atoms
    ['nil #"\xC0"]
    [#f #"\xC2"]
    [#t #"\xC3"]
    ;; positive fixint
    [0 #"\x00"]
    [#x7F #"\x7F"]
    ;; negative fixint
    [-1 #"\xFF"]
    [-32 #"\xE0"]
    ;; uint 8
    [#:unpack 0 #"\xCC\x00"]
    [#x80 #"\xCC\x80"]
    [#xFF #"\xCC\xFF"]
    ;; uint 16
    [#:unpack 0 #"\xCD\x00\x00"]
    [#x0100 #"\xCD\x01\x00"]
    [#xFFFF #"\xCD\xFF\xFF"]
    ;; uint 32
    [#:unpack 0 #"\xCE\x00\x00\x00\x00"]
    [#x01000000 #"\xCE\x01\x00\x00\x00"]
    [#xFFFFFFFF #"\xCE\xFF\xFF\xFF\xFF"]
    ;; uint 64
    [#:unpack 0 #"\xCF\x00\x00\x00\x00\x00\x00\x00\x00"]
    [#x0100000000000000 #"\xCF\x01\x00\x00\x00\x00\x00\x00\x00"]
    [#xFFFFFFFFFFFFFFFF #"\xCF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"]
    ;; int 8
    [#:unpack #x7F #"\xD0\x7F"]
    [#:unpack 0 #"\xD0\x00"]
    [#:unpack -1 #"\xD0\xFF"]
    [-33 #"\xD0\xDF"]
    [-128 #"\xD0\x80"]
    [#x-80 #"\xD0\x80"]
    ;; int 16
    [#:unpack #x7FFF #"\xD1\x7F\xFF"]
    [#:unpack 0 #"\xD1\x00\x00"]
    [#:unpack -1 #"\xD1\xFF\xFF"]
    [#x-81 #"\xD1\xFF\x7F"]
    [#x-8000 #"\xD1\x80\x00"]
    ;; int 32
    [#:unpack #x7FFFFFFF #"\xD2\x7F\xFF\xFF\xFF" ]
    [#:unpack 0 #"\xD2\x00\x00\x00\x00"]
    [#:unpack -1 #"\xD2\xFF\xFF\xFF\xFF"]
    [#x-8001 #"\xD2\xFF\xFF\x7F\xFF"]
    [#x-80000000 #"\xD2\x80\x00\x00\x00"]
    ;; int 64
    [#:unpack #x7FFFFFFFFFFFFFFF #"\xD3\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"]
    [#:unpack 0 #"\xD3\x00\x00\x00\x00\x00\x00\x00\x00"]
    [#:unpack -1 #"\xD3\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"]
    [#x-80000001 #"\xD3\xFF\xFF\xFF\xFF\x7F\xFF\xFF\xFF"]
    [#x-8000000000000000 #"\xD3\x80\x00\x00\x00\x00\x00\x00\x00"]
    ;; float 32
    [#:pack (real->single-flonum 1.0) #"\xCA\x3F\x80\x00\x00"]
    [#:unpack 1.0 #"\xCA\x3F\x80\x00\x00"]
    ;; float 64
    [(real->double-flonum 1.0) #"\xCB\x3F\xF0\x00\x00\x00\x00\x00\x00"]
    ;; fixmap
    [(hash) #"\x80"]
    ;; Note that we cannot test hashes with more than one association as the order of the
    ;; values is unspecified both when serializing and deserializing maps.
    [(hash 1 2) #"\x81\x01\x02"]))

;; Check packing of container types.
;;
;; - make-container is a function that takes a length argument and produces a container with the
;;   given length.
;; - container->bytes is a function that takes the container produced by make-container and
;;   transforms it into the desired data bytes for the MessagePack format.
(define-syntax-rule (check-containers make-container container->bytes [op ... len head] ...)
  (begin (let* ([container (make-container len)]
                [packed (bytes-append head (container->bytes container))])
           (check-msgpack op ... container packed)) ...))

(test-begin
  (check-containers
    make-bytes
    (lambda (v) v)
    ;; bin 8
    [0 #"\xC4\x00"]
    [#xFF #"\xC4\xFF"]
    ;; bin 16
    [#:unpack 0 #"\xC5\x00\x00"]
    [#x100 #"\xC5\x01\x00"]
    [#xFFFF #"\xC5\xFF\xFF"]
    ;; bin 32
    [#:unpack 0 #"\xC6\x00\x00\x00\x00"]
    [#x10000 #"\xC6\x00\x01\x00\x00"]))

(test-begin
  (check-containers
    make-string
    string->bytes/utf-8
    ;; fixstr
    [0 #"\xA0"]
    [#x1F #"\xBF"]
    ;; str 8
    [#:unpack 0 #"\xD9\x00"]
    [#x20 #"\xD9\x20"]
    [#xFF #"\xD9\xFF"]
    ;; str 16
    [#:unpack 0 #"\xDA\x00\x00"]
    [#x100 #"\xDA\x01\x00"]
    [#xFFFF #"\xDA\xFF\xFF"]
    ;; str 32
    [#:unpack 0 #"\xDB\x00\x00\x00\x00"]
    [#x10000 #"\xDB\x00\x01\x00\x00"]))

(test-begin
  (check-containers
    (lambda (len) (make-list len 0))
    (lambda (lst) (make-bytes (length lst)))
    ;; fixarray
    [0 #"\x90"]
    [#xF #"\x9F"]
    ;; array 16
    [#:unpack 0 #"\xDC\x00\x00"]
    [#x10 #"\xDC\x00\x10"]
    [#xFFFF #"\xDC\xFF\xFF"]
    ;; array 32
    [#:unpack 0 #"\xDD\x00\x00\x00\x00"]
    [#x10000 #"\xDD\x00\x01\x00\x00"]))

(test-begin
  (check-containers
    (lambda (len) (make-vector len 0))
    (lambda (vec) (make-bytes (vector-length vec)))
    ;; fixarray
    [#:pack 0 #"\x90"]
    [#:pack #xF #"\x9F"]
    ;; array 16
    [#:pack #x10 #"\xDC\x00\x10"]
    [#:pack #xFFFF #"\xDC\xFF\xFF"]
    ;; array 32
    [#:pack #x10000 #"\xDD\x00\x01\x00\x00"]))
