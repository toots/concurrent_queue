(* Assumption: deadlock can come from [Gc] collection
   happening in the same thread. *)

exception Empty = Queue.Empty

type 'a pending = ('a Queue.t -> unit) Queue.t

type 'a t = {
 queue:    'a Queue.t;
 pending:  (int, 'a pending) Hashtbl.t;
 mutex: Mutex.t;
}

let of_queue queue =
  let pending  = Hashtbl.create 10 in
  let mutex    = Mutex.create () in
  {queue;pending;mutex}

let create () =
  of_queue (Queue.create ())

let mutexify m fn =
  Mutex.lock m;
  try
    let ret = fn () in
    Mutex.unlock m;
    ret
  with exn ->
    Mutex.unlock m;
    raise exn

let id () =
  Thread.id (Thread.self ())

let rec flush q id =
  let pending =
    mutexify q.mutex (fun () ->
      match Hashtbl.find_opt q.pending id with
        | Some pending ->
            Hashtbl.remove q.pending id;
            pending
        | None ->
            assert false)
  in
  Queue.iter (fun fn -> exec_n fn q) pending
and exec_n fn q =
  let id = id () in
  let master =
    mutexify q.mutex (fun () ->
      match Hashtbl.find_opt q.pending id with
        | Some q ->
            Queue.add fn q;
            false
      | None ->
            Hashtbl.add q.pending id (Queue.create ());
            true)
  in
  if master then
   begin
    fn q.queue;
    flush q id
   end

let exec_v fn q =
  let id = id () in
  mutexify q.mutex (fun () ->
    match Hashtbl.find_opt q.pending id with
      | Some _ ->
          assert false
      | None ->
          Hashtbl.add q.pending id (Queue.create ()));
  let ret =
    fn q.queue
  in
  flush q id;
  ret

let add x = exec_n (Queue.add x)

let push = add

let take q = exec_v Queue.take q

let take_opt q = exec_v Queue.take_opt q

let pop = take

let peek q = exec_v Queue.peek q

let peek_opt q = exec_v Queue.peek_opt q

let top = peek

let clear q = exec_n Queue.clear q

let copy q = of_queue (exec_v Queue.copy q)

let is_empty q = exec_v Queue.is_empty q

let length q = exec_v Queue.length q

let iter fn = exec_n (Queue.iter fn)

let fold fn x q = exec_v (Queue.fold fn x) q

let transfer q1 q2 =
  exec_n (fun q1 ->
    exec_n (Queue.transfer q1) q2) q1

let to_seq q = exec_v Queue.to_seq q

let add_seq q = exec_v Queue.add_seq q 

let of_seq seq = of_queue (Queue.of_seq seq)
