(lang dune 3.5)

(name aeron)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Vincent Bernardoff")

(maintainers "Vincent Bernardoff")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name aeron)
 (synopsis "Bindings to Aeron Client")
 (description "A longer description")
 (depends ocaml dune)
 (tags
  (topics bindings)))

(package
 (name aeron_async)
 (synopsis "Bindings to Aeron Client")
 (description "A longer description")
 (depends ocaml dune core async)
 (tags
  (topics bindings)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
