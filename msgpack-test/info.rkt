#lang info

(define collection 'multi)
(define deps '("base"
               "git://github.com/eriknyh/msgpack-racket?path=msgpack-lib"
               "rackunit-lib"))
(define update-implies '("msgpack-lib"))
