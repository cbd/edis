%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for keys commands
%%% @end
%%%-------------------------------------------------------------------
-module(keys_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([del/1, exists/1, expire/1, expireat/1, keys/1, move/1, object_refcount/1,
         object_encoding/1, object_idletime/1, persist/1, randomkey/1, rename/1, renamenx/1,
         ttl/1, type/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec all() -> [atom()].
all() -> [Fun || {Fun, _} <- ?MODULE:module_info(exports) -- edis_bench:behaviour_info(callbacks),
                 Fun =/= module_info].

-spec init() -> ok.
init() -> ok.

-spec quit() -> ok.
quit() -> ok.

-spec init_per_testcase(atom()) -> ok.
init_per_testcase(_Function) -> ok.

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(_Function) -> ok.

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(move, Keys) ->
  edis_db:run(edis_db:process(1),
              #edis_command{cmd = <<"DEL">>, db = 1, args = Keys, result_type = ok, group = server}),
  [ok,ok] =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"EXEC">>, group = transaction, result_type = multi_result,
                    args = [#edis_command{cmd = <<"FLUSHDB">>, args = [], result_type = ok, group = server},
                            #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
                                          group = keys, result_type = ok}]}),
  ok;
init_per_round(keys, Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
                  group = keys, result_type = ok});
init_per_round(_Fun, Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, iolist_to_binary(Keys)} || Key <- Keys],
                  group = keys, result_type = ok}).

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(exists, Keys) -> del(Keys), ok;
quit_per_round(expire, Keys) -> del(Keys), ok;
quit_per_round(expireat, Keys) -> del(Keys), ok;
quit_per_round(move, Keys) -> del(Keys), ok;
quit_per_round(object_refcount, Keys) -> del(Keys), ok;
quit_per_round(object_encoding, Keys) -> del(Keys), ok;
quit_per_round(object_idletime, Keys) -> del(Keys), ok;
quit_per_round(persist, Keys) -> del(Keys), ok;
quit_per_round(rename, Keys) -> del([<<"test-new">>|Keys]), ok;
quit_per_round(renamenx, Keys) -> del([<<"test-new">>|Keys]), ok;
quit_per_round(ttl, Keys) -> del(Keys), ok;
quit_per_round(type, Keys) -> del(Keys), ok;
quit_per_round(_, _Keys) -> ok.

-spec del([binary()]) -> pos_integer().
del(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DEL">>, args = Keys, group = keys, result_type = number}).

-spec exists([binary(),...]) -> boolean().
exists([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"EXISTS">>, args = [Key], result_type = boolean, group = keys}).

-spec expire([binary(),...]) -> boolean().
expire([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"EXPIRE">>, args = [Key, edis_util:binary_to_integer(Key, 0)],
                  result_type = boolean, group = keys}).

-spec expireat([binary(),...]) -> boolean().
expireat([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"EXPIREAT">>, args = [Key, edis_util:binary_to_integer(Key, 0)],
                  result_type = boolean, group = keys}).

-spec keys([binary()]) -> pos_integer().
keys(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"KEYS">>, args = [<<".*">>], group = keys, result_type = multi_bulk}).

-spec move([binary(),...]) -> boolean().
move([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MOVE">>, args = [Key, edis_db:process(1)],
                  result_type = boolean, group = keys}).

-spec object_refcount([binary(),...]) -> integer().
object_refcount([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [Key], result_type = number, group = keys}).

-spec object_encoding([binary(),...]) -> binary().
object_encoding([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT ENCODING">>, args = [Key], result_type = bulk, group = keys}).

-spec object_idletime([binary(),...]) -> integer().
object_idletime([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT IDLETIME">>, args = [Key], result_type = number, group = keys}).

-spec persist([binary(),...]) -> boolean().
persist([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"PERSIST">>, args = [Key], result_type = boolean, group = keys}).

-spec randomkey([binary()]) -> binary().
randomkey(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"RANDOMKEY">>, args = [], group = keys, result_type = bulk}).

-spec rename([binary(),...]) -> boolean().
rename([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"RENAME">>, args = [Key, <<"test-new">>],
                  result_type = ok, group = keys}).

-spec renamenx([binary(),...]) -> boolean().
renamenx([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"RENAMENX">>, args = [Key, <<"test-new">>],
                  result_type = boolean, group = keys}).

-spec ttl([binary(),...]) -> boolean().
ttl([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"TTL">>, args = [Key], result_type = number, group = keys}).

-spec type([binary(),...]) -> boolean().
type([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"TYPE">>, args = [Key], result_type = string, group = keys}).
