MessagePack for Racket
======================

[MessagePack][] is a binary JSON-like serialization format. MessagePack for
Racket is a pure Racket implementation of MessagePack that facilitates writing
and reading to/from ports, as well as working with byte strings.


Installation
------------

    raco pkg install git://github.com/eriknyh/msgpack-racket?path=msgpack

[MessagePack]: http://msgpack.org/


Todo
----

- [ ] Handle errors while reading.
- [ ] Read arrays as vectors instead of lists. This is simply an optimization.
- [ ] Write vectors, lists and sequences as arrays. From this change vectors
  should be noted as the preferred type for array serialization as they do not
  have to be traversed to determine their length.
- [ ] Write any type that implements the dictionary interface as a map.
- [ ] Implement extension types.
