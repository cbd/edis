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
         object_encoding/1, object_idletime/1, persist/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec all() -> [atom()].
all() -> [del, exists, expire, expireat, keys, move, object_refcount, object_encoding,
          object_idletime, persist].

-spec init() -> ok.
init() -> ok.

-spec quit() -> ok.
quit() -> ok.

-spec init_per_testcase(atom()) -> ok.
init_per_testcase(_Function) -> ok.

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(_Function) -> ok.

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(Fun, Keys) when Fun =:= del;
                               Fun =:= exists;
                               Fun =:= expire;
                               Fun =:= expireat;
                               Fun =:= object_refcount;
                               Fun =:= object_encoding;
                               Fun =:= object_idletime;
                               Fun =:= persist ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
                  group = keys, result_type = ok});
init_per_round(move, Keys) ->
  [ok,ok] =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"EXEC">>, group = transaction, result_type = multi_result,
                    args = [#edis_command{cmd = <<"FLUSHDB">>, args = [], result_type = ok, group = server},
                            #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
                                          group = keys, result_type = ok}]}),
  ok;
init_per_round(keys, Keys) ->
  edis_db:run(edis_db:process(1),
              #edis_command{cmd = <<"FLUSHDB">>, db = 1, args = [], result_type = ok, group = server}),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
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
move(Keys) ->
  [Key|_] = lists:reverse(Keys),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MOVE">>, args = [Key, edis_db:process(1)],
                  result_type = boolean, group = keys}).

-spec object_refcount([binary(),...]) -> integer().
object_refcount(Keys) ->
  [Key|_] = lists:reverse(Keys),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [Key], result_type = number, group = keys}).

-spec object_encoding([binary(),...]) -> binary().
object_encoding(Keys) ->
  [Key|_] = lists:reverse(Keys),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT ENCODING">>, args = [Key], result_type = bulk, group = keys}).

-spec object_idletime([binary(),...]) -> integer().
object_idletime(Keys) ->
  [Key|_] = lists:reverse(Keys),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"OBJECT IDLETIME">>, args = [Key], result_type = number, group = keys}).

-spec persist([binary(),...]) -> boolean().
persist([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"PERSIST">>, args = [Key], result_type = boolean, group = keys}).
