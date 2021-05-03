open! Import

type t = {
  mutable bytes_read : int;
  mutable nb_reads : int;
  mutable bytes_written : int;
  mutable nb_writes : int;
  mutable nb_merge : int;
  mutable merge_durations : float list;
  mutable nb_replace : int;
  mutable replace_durations : float list;
  mutable nb_sync : int;
  mutable time_sync : float;
  mutable histo_read : Bentov.histogram;
  mutable histo_write : Bentov.histogram;
}

let fresh_stats () =
  let histo_read = Bentov.create 30 in
  let histo_write = Bentov.create 30 in
  {
    bytes_read = 0;
    nb_reads = 0;
    bytes_written = 0;
    nb_writes = 0;
    nb_merge = 0;
    merge_durations = [];
    nb_replace = 0;
    replace_durations = [];
    nb_sync = 0;
    time_sync = 0.0;
    histo_read;
    histo_write;
  }

let stats = fresh_stats ()

let reset_stats () =
  stats.bytes_read <- 0;
  stats.nb_reads <- 0;
  stats.bytes_written <- 0;
  stats.nb_writes <- 0;
  stats.nb_merge <- 0;
  stats.merge_durations <- [];
  stats.nb_replace <- 0;
  stats.replace_durations <- [];
  stats.nb_sync <- 0;
  stats.time_sync <- 0.0;
  stats.histo_read <- Bentov.create 30;
  stats.histo_write <- Bentov.create 30

let get () = stats
let incr_bytes_read n = stats.bytes_read <- stats.bytes_read + n
let incr_bytes_written n = stats.bytes_written <- stats.bytes_written + n
let incr_nb_reads () = stats.nb_reads <- succ stats.nb_reads
let incr_nb_writes () = stats.nb_writes <- succ stats.nb_writes
let incr_nb_merge () = stats.nb_merge <- succ stats.nb_merge
let incr_nb_replace () = stats.nb_replace <- succ stats.nb_replace
let incr_nb_sync () = stats.nb_sync <- succ stats.nb_sync

let add_read n duration =
  incr_bytes_read n;
  incr_nb_reads ();
  let point = Mtime.Span.to_us duration in
  stats.histo_read <- Bentov.add point stats.histo_read

let add_write n duration =
  incr_bytes_written n;
  incr_nb_writes ();
  let point = Mtime.Span.to_us duration in
  stats.histo_write <- Bentov.add point stats.histo_write

let replace_timer = ref (Mtime_clock.counter ())
let nb_replace = ref 0

let start_replace () =
  if !nb_replace = 0 then replace_timer := Mtime_clock.counter ()

let end_replace ~sampling_interval =
  nb_replace := !nb_replace + 1;
  if !nb_replace = sampling_interval then (
    let span = Mtime_clock.count !replace_timer in
    let average = Mtime.Span.to_us span /. float_of_int !nb_replace in
    stats.replace_durations <- average :: stats.replace_durations;
    replace_timer := Mtime_clock.counter ();
    nb_replace := 0)

let sync_with_timer f =
  let timer = Mtime_clock.counter () in
  f ();
  let span = Mtime_clock.count timer in
  stats.time_sync <- Mtime.Span.to_us span

let drop_head l = if List.length l >= 10 then List.tl l else l

let add_merge_duration span =
  let span = Mtime.Span.to_us span in
  stats.merge_durations <- drop_head stats.merge_durations @ [ span ]
