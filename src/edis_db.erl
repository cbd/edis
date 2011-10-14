%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Database
%%% @todo It's currently delivering all operations to the leveldb instance, i.e. no in-memory management
%%%       Therefore, operations like save/1 are not really implemented
%%% @todo We need to evaluate which calls should in fact be casts
%%% @todo We need to add info to INFO
%%% @end
%%%-------------------------------------------------------------------
-module(edis_db).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(gen_server).

-include("edis.hrl").
-define(DEFAULT_TIMEOUT, 5000).
-define(RANDOM_THRESHOLD, 500).

-type item_type() :: string | hash | list | set | zset.
-type item_encoding() :: raw | int | ziplist | linkedlist | intset | hashtable | zipmap | skiplist.
-export_type([item_encoding/0, item_type/0]).

-record(state, {index               :: non_neg_integer(),
                db                  :: eleveldb:db_ref(),
                start_time          :: pos_integer(),
                accesses            :: dict(),
                blocked_list_ops    :: dict(),
                last_save           :: float()}).
-opaque state() :: #state{}.

%% Administrative functions
-export([start_link/1, process/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Commands ========================================================================================
-export([ping/1, save/1, last_save/1, info/1, flush/0, flush/1, size/1]).
-export([append/3, decr/3, get/2, get_bit/3, get_range/4, get_and_set/3, incr/3, set/2, set/3,
         set_nx/2, set_nx/3, set_bit/4, set_ex/4, set_range/4, str_len/2]).
-export([del/2, exists/2, expire/3, expire_at/3, keys/2, move/3, encoding/2, idle_time/2, persist/2,
         random_key/1, rename/3, rename_nx/3, ttl/2, type/2]).
-export([hdel/3, hexists/3, hget/3, hget_all/2, hincr/4, hkeys/2, hlen/2, hset/3, hset/4, hset_nx/4, hvals/2]).
-export([blpop/3, brpop/3, brpop_lpush/4, lindex/3, linsert/5, llen/2, lpop/2, lpush/3, lpush_x/3,
         lrange/4, lrem/4, lset/4, ltrim/4, rpop/2, rpop_lpush/3, rpush/3, rpush_x/3]).
-export([sadd/3, scard/2]).

%% =================================================================================================
%% External functions
%% =================================================================================================
-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(Index) ->
  gen_server:start_link({local, process(Index)}, ?MODULE, Index, []).

-spec process(non_neg_integer()) -> atom().
process(Index) ->
  list_to_atom("edis-db-" ++ integer_to_list(Index)).

%% =================================================================================================
%% Commands
%% =================================================================================================
-spec size(atom()) -> non_neg_integer().
size(Db) ->
  make_call(Db, size).

-spec flush() -> ok.
flush() ->
  lists:foreach(
    fun flush/1, [process(Index) || Index <- lists:seq(0, edis_config:get(databases) - 1)]).

-spec flush(atom()) -> ok.
flush(Db) ->
  make_call(Db, flush).

-spec ping(atom()) -> pong.
ping(Db) ->
  make_call(Db, ping).

-spec save(atom()) -> ok.
save(Db) ->
  make_call(Db, save).

-spec last_save(atom()) -> ok.
last_save(Db) ->
  make_call(Db, last_save).

-spec info(atom()) -> [{atom(), term()}].
info(Db) ->
  make_call(Db, info).

-spec append(atom(), binary(), binary()) -> pos_integer().
append(Db, Key, Value) ->
  make_call(Db, {append, Key, Value}).

-spec decr(atom(), binary(), integer()) -> integer().
decr(Db, Key, Decrement) ->
  make_call(Db, {decr, Key, Decrement}).

-spec get(atom(), binary()|[binary()]) -> undefined | binary().
get(Db, Key) when is_binary(Key) ->
  [Value] = get(Db, [Key]),
  Value;
get(Db, Keys) ->
  make_call(Db, {get, Keys}).

-spec get_bit(atom(), binary(), non_neg_integer()) -> 1|0.
get_bit(Db, Key, Offset) ->
  make_call(Db, {get_bit, Key, Offset}).

-spec get_range(atom(), binary(), integer(), integer()) -> binary().
get_range(Db, Key, Start, End) ->
  make_call(Db, {get_range, Key, Start, End}).

-spec get_and_set(atom(), binary(), binary()) -> undefined | binary().
get_and_set(Db, Key, Value) ->
  make_call(Db, {get_and_set, Key, Value}).

-spec incr(atom(), binary(), integer()) -> integer().
incr(Db, Key, Increment) ->
  make_call(Db, {incr, Key, Increment}).

-spec set(atom(), binary(), binary()) -> ok.
set(Db, Key, Value) ->
  set(Db, [{Key, Value}]).

-spec set(atom(), [{binary(), binary()}]) -> ok.
set(Db, KVs) ->
  make_call(Db, {set, KVs}).

-spec set_nx(atom(), binary(), binary()) -> ok.
set_nx(Db, Key, Value) ->
  set_nx(Db, [{Key, Value}]).

-spec set_nx(atom(), [{binary(), binary()}]) -> ok.
set_nx(Db, KVs) ->
  make_call(Db, {set_nx, KVs}).

-spec set_bit(atom(), binary(), non_neg_integer(), 1|0) -> 1|0.
set_bit(Db, Key, Offset, Bit) ->
  make_call(Db, {set_bit, Key, Offset, Bit}).

-spec set_ex(atom(), binary(), pos_integer(), binary()) -> ok.
set_ex(Db, Key, Seconds, Value) ->
  make_call(Db, {set_ex, Key, Seconds, Value}).

-spec set_range(atom(), binary(), pos_integer(), binary()) -> non_neg_integer().
set_range(Db, Key, Offset, Value) ->
  make_call(Db, {set_range, Key, Offset, Value}).

-spec str_len(atom(), binary()) -> non_neg_integer().
str_len(Db, Key) ->
  make_call(Db, {str_len, Key}).

-spec del(atom(), binary()) -> non_neg_integer().
del(Db, Keys) ->
  make_call(Db, {del, Keys}).

-spec exists(atom(), binary()) -> boolean().
exists(Db, Key) ->
  make_call(Db, {exists, Key}).

-spec expire(atom(), binary(), pos_integer()) -> boolean().
expire(Db, Key, Seconds) ->
  expire_at(Db, Key, edis_util:now() + Seconds).

-spec expire_at(atom(), binary(), pos_integer()) -> boolean().
expire_at(Db, Key, Timestamp) ->
  make_call(Db, {expire_at, Key, Timestamp}).

-spec keys(atom(), binary()) -> [binary()].
keys(Db, Pattern) ->
  make_call(Db, {keys, Pattern}).

-spec move(atom(), binary(), atom()) -> boolean().
move(Db, Key, NewDb) ->
  make_call(Db, {move, Key, NewDb}).

-spec encoding(atom(), binary()) -> undefined | item_encoding().
encoding(Db, Key) ->
  make_call(Db, {encoding, Key}).

-spec idle_time(atom(), binary()) -> undefined | non_neg_integer().
idle_time(Db, Key) ->
  make_call(Db, {idle_time, Key}).

-spec persist(atom(), binary()) -> boolean().
persist(Db, Key) ->
  make_call(Db, {persist, Key}).

-spec random_key(atom()) -> undefined | binary().
random_key(Db) ->
  make_call(Db, random_key).

-spec rename(atom(), binary(), binary()) -> ok.
rename(Db, Key, NewKey) ->
  make_call(Db, {rename, Key, NewKey}).

-spec rename_nx(atom(), binary(), binary()) -> ok.
rename_nx(Db, Key, NewKey) ->
  make_call(Db, {rename_nx, Key, NewKey}).

-spec ttl(atom(), binary()) -> undefined | pos_integer().
ttl(Db, Key) ->
  make_call(Db, {ttl, Key}).

-spec type(atom(), binary()) -> item_type().
type(Db, Key) ->
  make_call(Db, {type, Key}).

-spec hdel(atom(), binary(), [binary()]) -> non_neg_integer().
hdel(Db, Key, Fields) ->
  make_call(Db, {hdel, Key, Fields}).

-spec hexists(atom(), binary(), binary()) -> boolean().
hexists(Db, Key, Field) ->
  make_call(Db, {hexists, Key, Field}).

-spec hget(atom(), binary(), binary() | [binary()]) -> undefined | binary() | [undefined | binary()].
hget(Db, Key, Fields) when is_list(Fields) ->
  make_call(Db, {hget, Key, Fields});
hget(Db, Key, Field) ->
  [Res] = make_call(Db, {hget, Key, [Field]}),
  Res.

-spec hget_all(atom(), binary()) -> [{binary(), binary()}].
hget_all(Db, Key) ->
  make_call(Db, {hget_all, Key}).

-spec hincr(atom(), binary(), binary(), integer()) -> inserted | updated.
hincr(Db, Key, Field, Increment) ->
  make_call(Db, {hincr, Key, Field, Increment}).

-spec hkeys(atom(), binary()) -> [binary()].
hkeys(Db, Key) ->
  make_call(Db, {hkeys, Key}).

-spec hlen(atom(), binary()) -> non_neg_integer().
hlen(Db, Key) ->
  make_call(Db, {hlen, Key}).

-spec hset(atom(), binary(), [{binary(), binary()}]) -> inserted | updated.
hset(Db, Key, FVs) ->
  make_call(Db, {hset, Key, FVs}).

-spec hset(atom(), binary(), binary(), binary()) -> inserted | updated.
hset(Db, Key, Field, Value) ->
  hset(Db, Key, [{Field, Value}]).

-spec hset_nx(atom(), binary(), binary(), binary()) -> ok.
hset_nx(Db, Key, Field, Value) ->
  make_call(Db, {hset_nx, Key, Field, Value}).

-spec hvals(atom(), binary()) -> [binary()].
hvals(Db, Key) ->
  make_call(Db, {hvals, Key}).

-spec blpop(atom(), [binary()], infinity | non_neg_integer()) -> {binary(), binary()}.
blpop(Db, Keys, Timeout) ->
  make_call(Db, {blpop, Keys, timeout_to_seconds(Timeout)}, timeout_to_ms(Timeout)).

-spec brpop(atom(), [binary()], infinity | non_neg_integer()) -> {binary(), binary()}.
brpop(Db, Keys, Timeout) ->
  make_call(Db, {brpop, Keys, timeout_to_seconds(Timeout)}, timeout_to_ms(Timeout)).

-spec brpop_lpush(atom(), binary(), binary(), infinity | non_neg_integer()) -> binary().
brpop_lpush(Db, Source, Destination, Timeout) ->
  make_call(Db, {brpop_lpush, Source, Destination, timeout_to_seconds(Timeout)},
            timeout_to_ms(Timeout)).

-spec lindex(atom(), binary(), integer()) -> undefined | binary().
lindex(Db, Key, Index) ->
  make_call(Db, {lindex, Key, Index}).

-spec linsert(atom(), binary(), before|'after', binary(), binary()) -> -1 | non_neg_integer().
linsert(Db, Key, Position, Pivot, Value) ->
  make_call(Db, {linsert, Key, Position, Pivot, Value}).

-spec llen(atom(), binary()) -> non_neg_integer().
llen(Db, Key) ->
  make_call(Db, {llen, Key}).

-spec lpop(atom(), binary()) -> binary().
lpop(Db, Key) ->
  make_call(Db, {lpop, Key}).

-spec lpush(atom(), binary(), binary()) -> pos_integer().
lpush(Db, Key, Value) ->
  make_call(Db, {lpush, Key, Value}).

-spec lpush_x(atom(), binary(), binary()) -> pos_integer().
lpush_x(Db, Key, Value) ->
  make_call(Db, {lpush_x, Key, Value}).

-spec lrange(atom(), binary(), integer(), integer()) -> ok.
lrange(Db, Key, Start, Stop) ->
  make_call(Db, {lrange, Key, Start, Stop}).

-spec lrem(atom(), binary(), integer(), binary()) -> ok.
lrem(Db, Key, Count, Value) ->
  make_call(Db, {lrem, Key, Count, Value}).

-spec lset(atom(), binary(), integer(), binary()) -> ok.
lset(Db, Key, Index, Value) ->
  make_call(Db, {lset, Key, Index, Value}).

-spec ltrim(atom(), binary(), integer(), integer()) -> ok.
ltrim(Db, Key, Start, Stop) ->
  make_call(Db, {ltrim, Key, Start, Stop}).

-spec rpop(atom(), binary()) -> binary().
rpop(Db, Key) ->
  make_call(Db, {rpop, Key}).

-spec rpop_lpush(atom(), binary(), binary()) -> binary().
rpop_lpush(Db, Key, Value) ->
  make_call(Db, {rpop_lpush, Key, Value}).

-spec rpush(atom(), binary(), binary()) -> pos_integer().
rpush(Db, Key, Value) ->
  make_call(Db, {rpush, Key, Value}).

-spec rpush_x(atom(), binary(), binary()) -> pos_integer().
rpush_x(Db, Key, Value) ->
  make_call(Db, {rpush_x, Key, Value}).

-spec sadd(atom(), binary(), [binary()]) -> non_neg_integer().
sadd(Db, Key, Members) ->
  make_call(Db, {sadd, Key, Members}).

-spec scard(atom(), binary()) -> non_neg_integer().
scard(Db, Key) ->
  make_call(Db, {scard, Key}).

%% =================================================================================================
%% Server functions
%% =================================================================================================
%% @hidden
-spec init(non_neg_integer()) -> {ok, state()} | {stop, any()}.
init(Index) ->
  _ = random:seed(erlang:now()),
  case eleveldb:open("db/edis-" ++ integer_to_list(Index), [{create_if_missing, true}]) of
    {ok, Ref} ->
      {ok, #state{index = Index, db = Ref, last_save = edis_util:timestamp(),
                  start_time = edis_util:now(), accesses = dict:new(),
                  blocked_list_ops = orddict:new()}};
    {error, Reason} ->
      ?THROW("Couldn't start level db #~p:~b\t~p~n", [Index, Reason]),
      {stop, Reason}
  end.

%% @hidden
-spec handle_call(term(), reference(), state()) -> {reply, ok | {ok, term()} | {error, term()}, state()} | {stop, {unexpected_request, term()}, {unexpected_request, term()}, state()}.
handle_call(save, _From, State) ->
  {reply, ok, State#state{last_save = edis_util:timestamp()}};
handle_call(last_save, _From, State) ->
  {reply, {ok, State#state.last_save}, State};
handle_call(ping, _From, State) ->
  {reply, {ok, pong}, State};
handle_call(info, _From, State) ->
  Version =
    case lists:keyfind(edis, 1, application:loaded_applications()) of
      false -> "0";
      {edis, _Desc, V} -> V
    end,
  {ok, Stats} = eleveldb:status(State#state.db, <<"leveldb.stats">>),
  {reply, {ok, [{edis_version, Version},
                {last_save, State#state.last_save},
                {db_stats, Stats}]}, %%TODO: add info
   State};
handle_call(flush, _From, State) ->
  ok = eleveldb:destroy("db/edis-" ++ integer_to_list(State#state.index), []),
  case init(State#state.index) of
    {ok, NewState} ->
      {reply, ok, NewState};
    {stop, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call(size, _From, State) ->
  %%TODO: We need to 
  Now = edis_util:now(),
  Size = eleveldb:fold(
           State#state.db,
           fun({_Key, Bin}, Acc) ->
                   case erlang:binary_to_term(Bin) of
                     #edis_item{expire = Expire} when Expire >= Now ->
                       Acc + 1;
                     _ ->
                       Acc
                   end
           end, 0, [{fill_cache, false}]),
  {reply, {ok, Size}, State};
handle_call({append, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   NewV = <<OldV/binary, Value/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}}
           end, <<>>),
  {reply, Reply, stamp(Key, State)};
handle_call({decr, Key, Decrement}, _From, State) ->
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   try edis_util:binary_to_integer(OldV) of
                     OldInt ->
                       Res = OldInt - Decrement,
                       {Res, Item#edis_item{value = edis_util:integer_to_binary(Res)}}
                   catch
                     _:badarg ->
                       throw(not_integer)
                   end
           end, <<"0">>),
  {reply, Reply, stamp(Key, State)};
handle_call({get, Keys}, _From, State) ->
  Reply =
    lists:foldr(
      fun(Key, {ok, AccValues}) ->
              case get_item(State#state.db, string, Key) of
                #edis_item{type = string, value = Value} -> {ok, [Value | AccValues]};
                not_found -> {ok, [undefined | AccValues]};
                {error, bad_item_type} -> {ok, [undefined | AccValues]};
                {error, Reason} -> {error, Reason}
              end;
         (_, AccErr) -> AccErr
      end, {ok, []}, Keys),
  {reply, Reply, stamp(Keys, State)};
handle_call({get_bit, Key, Offset}, _From, State) ->
  Reply =
    case get_item(State#state.db, string, Key) of
      #edis_item{value =
                   <<_:Offset/unit:1, Bit:1/unit:1, _Rest/bitstring>>} -> {ok, Bit};
      #edis_item{} -> {ok, 0}; %% Value is shorter than offset
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({get_range, Key, Start, End}, _From, State) ->
  Reply =
    try
      case get_item(State#state.db, string, Key) of
        #edis_item{value = Value} ->
          L = erlang:size(Value),
          StartPos =
            case Start of
              Start when Start >= L -> throw(empty);
              Start when Start >= 0 -> Start;
              Start when Start < (-1)*L -> 0;
              Start -> L + Start
            end,
          EndPos =
            case End of
              End when End >= 0, End >= L -> L - 1;
              End when End >= 0 -> End;
              End when End < (-1)*L -> 0;
              End -> L + End
            end,
          case EndPos - StartPos + 1 of
            Len when Len =< 0 -> {ok, <<>>};
            Len -> {ok, binary:part(Value, StartPos, Len)}
          end;
        not_found -> {ok, <<>>};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, <<>>}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({get_and_set, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   {OldV, Item#edis_item{value = Value}}
           end, undefined),
  {reply, Reply, stamp(Key, State)};
handle_call({incr, Key, Increment}, _From, State) ->
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   try edis_util:binary_to_integer(OldV) of
                     OldInt ->
                       Res = OldInt + Increment,
                       {Res, Item#edis_item{value = edis_util:integer_to_binary(Res)}}
                   catch
                     _:badarg ->
                       throw(not_integer)
                   end
           end, <<"0">>),
  {reply, Reply, stamp(Key, State)};
handle_call({set, KVs}, _From, State) ->
  Reply =
    eleveldb:write(State#state.db,
                   [{put, Key,
                     erlang:term_to_binary(
                       #edis_item{key = Key, encoding = raw,
                                  type = string, value = Value})} || {Key, Value} <- KVs],
                    []),
  {reply, Reply, stamp([K || {K, _} <- KVs], State)};
handle_call({set_nx, KVs}, _From, State) ->
  Reply =
    case lists:any(
           fun({Key, _}) ->
                   exists_item(State#state.db, Key)
           end, KVs) of
      true ->
        {error, already_exists};
      false ->
        eleveldb:write(State#state.db,
                       [{put, Key,
                         erlang:term_to_binary(
                           #edis_item{key = Key, encoding = raw,
                                      type = string, value = Value})} || {Key, Value} <- KVs],
                       [])
    end,
  {reply, Reply, stamp([K || {K, _} <- KVs], State)};
handle_call({set_bit, Key, Offset, Bit}, _From, State) ->
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = <<Prefix:Offset/unit:1, OldBit:1/unit:1, _Rest/bitstring>>}) ->
                   {OldBit,
                    Item#edis_item{value = <<Prefix:Offset/unit:1, Bit:1/unit:1, _Rest/bitstring>>}};
              (Item) when Bit == 0 -> %% Value is shorter than offset
                   {0, Item};
              (Item = #edis_item{value = Value}) when Bit == 1 -> %% Value is shorter than offset
                   BitsBefore = Offset - (erlang:size(Value) * 8),
                   BitsAfter = 7 - (Offset rem 8),
                   {0, Item#edis_item{value = <<Value/bitstring, 
                                                0:BitsBefore/unit:1,
                                                1:1/unit:1,
                                                0:BitsAfter/unit:1>>}}
           end, <<>>),
  {reply, Reply, stamp(Key, State)};
handle_call({set_ex, Key, Seconds, Value}, _From, State) ->
  Reply =
    eleveldb:put(
      State#state.db, Key,
      erlang:term_to_binary(
        #edis_item{key = Key, type = string, encoding = raw,
                   expire = edis_util:now() + Seconds,
                   value = Value}), []),
  {reply, Reply, stamp(Key, State)};
handle_call({set_range, Key, Offset, Value}, _From, State) ->
  Length = erlang:size(Value),
  Reply =
    update(State#state.db, Key, string, raw,
           fun(Item = #edis_item{value = <<Prefix:Offset/binary, _:Length/binary, Suffix/binary>>}) ->
                   NewV = <<Prefix/binary, Value/binary, Suffix/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}};
              (Item = #edis_item{value = <<Prefix:Offset/binary, _/binary>>}) ->
                   NewV = <<Prefix/binary, Value/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}};
              (Item = #edis_item{value = Prefix}) ->
                   Pad = Offset - erlang:size(Prefix),
                   NewV = <<Prefix/binary, 0:Pad/unit:8, Value/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}}
           end, <<>>),
  {reply, Reply, stamp(Key, State)};
handle_call({str_len, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, string, Key) of
      #edis_item{value = Value} -> {ok, erlang:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({del, Keys}, _From, State) ->
  DeleteActions =
      [{delete, Key} || Key <- Keys, exists_item(State#state.db, Key)],
  Reply =
    case eleveldb:write(State#state.db, DeleteActions, []) of
      ok -> {ok, length(DeleteActions)};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Keys, State)};
handle_call({exists, Key}, _From, State) ->
  Reply =
      case exists_item(State#state.db, Key) of
        true -> {ok, true};
        false -> {ok, false};
        {error, Reason} -> {error, Reason}
      end,
  {reply, Reply, stamp(Key, State)};
handle_call({expire_at, Key, Timestamp}, _From, State) ->
  Reply =
      case edis_util:now() of
        Now when Timestamp =< Now -> %% It's a delete (it already expired)
          case exists_item(State#state.db, Key) of
            true ->
              case eleveldb:delete(State#state.db, Key, []) of
                ok ->
                  {ok, true};
                {error, Reason} ->
                  {error, Reason}
              end;
            false ->
              {ok, false}
          end;
        _ ->
          case update(State#state.db, Key, any,
                      fun(Item) ->
                              {ok, Item#edis_item{expire = Timestamp}}
                      end) of
            {ok, ok} ->
              {ok, true};
            {error, not_found} ->
              {ok, false};
            {error, Reason} ->
              {error, Reason}
          end
      end,
  {reply, Reply, stamp(Key, State)};
handle_call({keys, Pattern}, _From, State) ->
  Reply =
    case re:compile(Pattern) of
      {ok, Compiled} ->
        Now = edis_util:now(),
        Keys = eleveldb:fold(
                 State#state.db,
                 fun({Key, Bin}, Acc) ->
                         case re:run(Key, Compiled) of
                           nomatch ->
                             Acc;
                           _ ->
                             case erlang:binary_to_term(Bin) of
                               #edis_item{expire = Expire} when Expire >= Now ->
                                 [Key | Acc];
                               _ ->
                                 Acc
                             end
                         end
                 end, [], [{fill_cache, false}]),
        {ok, lists:reverse(Keys)};
      {error, {Reason, _Line}} when is_list(Reason) ->
        {error, "Invalid pattern: " ++ Reason};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, State};
handle_call({move, Key, NewDb}, _From, State) ->
  Reply =
    case get_item(State#state.db, string, Key) of
      not_found ->
        {ok, false};
      {error, Reason} ->
        {error, Reason};
      Item ->
        try make_call(NewDb, {recv, Item}) of
          ok ->
            case eleveldb:delete(State#state.db, Key, []) of
              ok ->
                {ok, true};
              {error, Reason} ->
                _ = make_call(NewDb, {del, [Key]}),
                {error, Reason}
            end
        catch
          _:found -> {ok, false};
          _:{error, Reason} -> {error, Reason}
        end
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({recv, Item}, _From, State) ->
  Reply =
    case exists_item(State#state.db, Item#edis_item.key) of
      true -> {error, found};
      false -> eleveldb:put(State#state.db, Item#edis_item.key, erlang:term_to_binary(Item), [])
    end,
  {reply, Reply, stamp(Item#edis_item.key, State)};
handle_call({encoding, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, any, Key) of
      #edis_item{encoding = Encoding} -> {ok, Encoding};
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, State};
handle_call({idle_time, Key}, _From, State) ->
  Reply =
    case exists_item(State#state.db, Key) of
      true ->
        Offset =
          case dict:find(Key, State#state.accesses) of
            {ok, O} -> O;
            error -> 0
          end,
        {ok, edis_util:now() - Offset - State#state.start_time};
      false -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, State};
handle_call({persist, Key}, _From, State) ->
  Reply =
    case update(State#state.db, Key, any,
                fun(Item) ->
                        {ok, Item#edis_item{expire = infinity}}
                end) of
      {ok, ok} ->
        {ok, true};
      {error, not_found} ->
        {ok, false};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call(random_key, _From, State) ->
  Reply =
    case eleveldb:is_empty(State#state.db) of
      true -> undefined;
      false ->
        %%TODO: Make it really random... not just on the first xx tops
        %%      BUT we need to keep it O(1)
        RandomIndex = random:uniform(?RANDOM_THRESHOLD),
        key_at(State#state.db, RandomIndex)
    end,
  {reply, Reply, State};
handle_call({rename, Key, NewKey}, _From, State) ->
  Reply =
    case get_item(State#state.db, any, Key) of
      not_found ->
        {error, not_found};
      {error, Reason} ->
        {error, Reason};
      Item ->
        eleveldb:write(State#state.db,
                       [{delete, Key},
                        {put, NewKey,
                         erlang:term_to_binary(Item#edis_item{key = NewKey})}],
                       [])
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({rename_nx, Key, NewKey}, _From, State) ->
  Reply =
    case get_item(State#state.db, any, Key) of
      not_found ->
        {error, not_found};
      {error, Reason} ->
        {error, Reason};
      Item ->
        case exists_item(State#state.db, NewKey) of
          true ->
            {error, already_exists};
          false ->
            eleveldb:write(State#state.db,
                           [{delete, Key},
                            {put, NewKey,
                             erlang:term_to_binary(Item#edis_item{key = NewKey})}],
                           [])
        end
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({ttl, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, any, Key) of
      not_found ->
        {error, not_found};
      #edis_item{expire = infinity} ->
        {ok, undefined};
      Item ->
        {ok, Item#edis_item.expire - edis_util:now()}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({type, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, any, Key) of
      not_found ->
        {error, not_found};
      Item ->
        {ok, Item#edis_item.type}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hdel, Key, Fields}, _From, State) ->
  Reply =
    case update(State#state.db, Key, hash,
                fun(Item) ->
                        NewDict = lists:foldl(fun dict:erase/2, Item#edis_item.value, Fields),
                        {{dict:size(Item#edis_item.value) - dict:size(NewDict),
                          dict:size(NewDict)},
                         Item#edis_item{value = NewDict}}
                end) of
      {ok, {Deleted, 0}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Deleted};
      {ok, {Deleted, _}} ->
        {ok, Deleted};
      {error, not_found} ->
        {ok, 0};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hexists, Key, Field}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, false};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:is_key(Field, Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hget, Key, Fields}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason};
      Item ->
        Results =
          lists:map(
            fun(Field) ->
                    case dict:find(Field, Item#edis_item.value) of
                      {ok, Value} -> Value;
                      error -> undefined
                    end
            end, Fields),
        {ok, Results}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hget_all, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:to_list(Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hincr, Key, Field, Increment}, _From, State) ->
  Reply =
    update(
      State#state.db, Key, hash, hashtable,
      fun(Item) ->
              case dict:find(Field, Item#edis_item.value) of
                error ->
                  {Increment,
                   Item#edis_item{value =
                                    dict:store(Field,
                                               edis_util:integer_to_binary(Increment),
                                               Item#edis_item.value)}};
                {ok, OldValue} ->
                  try edis_util:binary_to_integer(OldValue) of
                    OldInt ->
                      {OldInt + Increment,
                       Item#edis_item{value =
                                        dict:store(Field,
                                                   edis_util:integer_to_binary(OldInt + Increment),
                                                   Item#edis_item.value)}}
                  catch
                    _:badarg ->
                      throw(not_integer)
                  end
              end
      end, dict:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({hkeys, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:fetch_keys(Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hlen, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:size(Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({hset, Key, FVs}, _From, State) ->
  Reply =
    update(
      State#state.db, Key, hash, hashtable,
      fun(Item) ->
              lists:foldl(
                fun({Field, Value}, {AccStatus, AccItem}) ->
                        case dict:is_key(Field, Item#edis_item.value) of
                          true ->
                            {AccStatus,
                             Item#edis_item{value =
                                              dict:store(Field, Value, AccItem#edis_item.value)}};
                          false ->
                            {inserted,
                             Item#edis_item{value =
                                              dict:store(Field, Value, AccItem#edis_item.value)}}
                        end
                end, {updated, Item}, FVs)
      end, dict:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({hset_nx, Key, Field, Value}, _From, State) ->
  Reply =
    update(
      State#state.db, Key, hash, hashtable,
      fun(Item) ->
              case dict:is_key(Field, Item#edis_item.value) of
                true ->
                  throw(already_exists);
                false ->
                  {ok, Item#edis_item{value = dict:store(Field, Value, Item#edis_item.value)}}
                end
      end, dict:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({hvals, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:fold(fun(_,Value,Acc) ->
                                     [Value|Acc]
                             end, [], Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({blpop, Keys, Timeout}, From, State) ->
  Reqs = [{blpop_internal, Key, []} || Key <- Keys],
  case first_that_works(Reqs, From, State) of
    {reply, {error, not_found}, NewState} ->
      {noreply,
       lists:foldl(
         fun(Key, AccState) ->
                 block_list_op(Key, {blpop_internal, Key, Keys}, From, Timeout, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call({blpop_internal, Key, Keys}, From, State) ->
  case handle_call({lpop, Key}, From, State) of
    {reply, {ok, Value}, NewState} ->
      {reply, {ok, {Key, Value}},
       lists:foldl(
         fun(K, AccState) ->
                 unblock_list_ops(K, From, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call({brpop, Keys, Timeout}, From, State) ->
  Reqs = [{brpop_internal, Key, []} || Key <- Keys],
  case first_that_works(Reqs, From, State) of
    {reply, {error, not_found}, NewState} ->
      {noreply,
       lists:foldl(
         fun(Key, AccState) ->
                 block_list_op(Key, {brpop_internal, Key, Keys}, From, Timeout, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call({brpop_internal, Key, Keys}, From, State) ->
  case handle_call({rpop, Key}, From, State) of
    {reply, {ok, Value}, NewState} ->
      {reply, {ok, {Key, Value}},
       lists:foldl(
         fun(K, AccState) ->
                 unblock_list_ops(K, From, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call({brpop_lpush, Source, Destination, Timeout}, From, State) ->
  Req = {rpop_lpush, Source, Destination},
  case handle_call(Req, From, State) of
    {reply, {error, not_found}, NewState} ->
      {noreply, block_list_op(Source, Req, From, Timeout, NewState)};
    OtherReply ->
      OtherReply
  end;
handle_call({lindex, Key, Index}, _From, State) ->
  Reply =
    case get_item(State#state.db, list, Key) of
      #edis_item{value = Value} ->
        try
          case Index of
            Index when Index >= 0 ->
              {ok, lists:nth(Index + 1, Value)};
            Index ->
              {ok, lists:nth((-1)*Index, Value)}
          end
        catch
          _:function_clause ->
            {ok, undefined}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({linsert, Key, Position, Pivot, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, list,
           fun(Item) ->
                   case {lists:splitwith(fun(Val) ->
                                                Val =/= Pivot
                                        end, Item#edis_item.value), Position} of
                     {{_, []}, _} -> %% Value was not found
                       {-1, Item};
                     {{Before, After}, before} ->
                       {length(Item#edis_item.value) + 1,
                        Item#edis_item{value = lists:append(Before, [Value|After])}};
                     {{Before, [Pivot|After]}, 'after'} ->
                       {length(Item#edis_item.value) + 1,
                        Item#edis_item{value = lists:append(Before, [Pivot, Value|After])}}
                   end
           end),
  {reply, Reply, stamp(Key, State)};
handle_call({llen, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, list, Key) of
      #edis_item{value = Value} -> {ok, length(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({lpop, Key}, _From, State) ->
  Reply =
    case update(State#state.db, Key, list,
                fun(Item) ->
                        case Item#edis_item.value of
                          [Value] ->
                            {{delete, Value}, Item#edis_item{value = []}};
                          [Value|Rest] ->
                            {{keep, Value}, Item#edis_item{value = Rest}};
                          [] ->
                            throw(not_found)
                        end
                end) of
      {ok, {delete, Value}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Value};
      {ok, {keep, Value}} ->
        {ok, Value};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({lpush, Key, Value}, From, State) ->
  Reply =
    update(State#state.db, Key, list, linkedlist,
           fun(Item) ->
                   {length(Item#edis_item.value) + 1,
                    Item#edis_item{value = [Value | Item#edis_item.value]}}
           end, []),
  gen_server:reply(From, Reply),
  NewState = check_blocked_list_ops(Key, State),
  {noreply, stamp(Key, NewState)};
handle_call({lpush_x, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, list,
           fun(Item) ->
                   {length(Item#edis_item.value) + 1,
                    Item#edis_item{value = [Value | Item#edis_item.value]}}
           end),
  {reply, Reply, stamp(Key, State)};
handle_call({lrange, Key, Start, Stop}, _From, State) ->
  Reply =
    try
      case get_item(State#state.db, list, Key) of
        #edis_item{value = Value} ->
          L = erlang:length(Value),
          StartPos =
            case Start of
              Start when Start >= L -> throw(empty);
              Start when Start >= 0 -> Start + 1;
              Start when Start < (-1)*L -> 1;
              Start -> L + 1 + Start
            end,
          StopPos =
            case Stop of
              Stop when Stop >= 0, Stop >= L -> L;
              Stop when Stop >= 0 -> Stop + 1;
              Stop when Stop < (-1)*L -> 0;
              Stop -> L + 1 + Stop
            end,
          case StopPos - StartPos + 1 of
            Len when Len =< 0 -> {ok, []};
            Len -> {ok, lists:sublist(Value, StartPos, Len)}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({lrem, Key, Count, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, list,
           fun(Item) ->
                   NewV =
                     case Count of
                       0 ->
                         lists:filter(fun(Val) ->
                                              Val =/= Value
                                      end, Item#edis_item.value);
                       Count when Count >= 0 ->
                         Item#edis_item.value -- lists:duplicate(Count, Value);
                       Count ->
                         lists:reverse(
                           lists:reverse(Item#edis_item.value) --
                             lists:duplicate((-1)*Count, Value))
                     end,
                   {length(Item#edis_item.value) - length(NewV), Item#edis_item{value = NewV}}
           end),
  {reply, Reply, stamp(Key, State)};
handle_call({lset, Key, Index, Value}, _From, State) ->
  Reply =
    case
      update(State#state.db, Key, list,
             fun(Item) ->
                     case Index of
                       0 ->
                         {ok, Item#edis_item{value = [Value | erlang:tl(Item#edis_item.value)]}};
                       -1 ->
                         [_|Rest] = lists:reverse(Item#edis_item.value),
                         {ok, Item#edis_item{value = lists:reverse([Value|Rest])}};
                       Index when Index >= 0 ->
                         case lists:split(Index, Item#edis_item.value) of
                           {Before, [_|After]} ->
                             {ok, Item#edis_item{value = lists:append(Before, [Value|After])}};
                           {_, []} ->
                             throw(badarg)
                         end;
                       Index ->
                         {RAfter, RBefore} =
                           lists:split((-1)*Index, lists:reverse(Item#edis_item.value)),
                         [_|After] = lists:reverse(RAfter),
                         {ok, Item#edis_item{value =
                                               lists:append(lists:reverse(RBefore), [Value|After])}}
                     end
             end) of
      {ok, ok} -> ok;
      {error, badarg} -> {error, out_of_range};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({ltrim, Key, Start, Stop}, _From, State) ->
  Reply =
    case update(State#state.db, Key, list,
                fun(Item) ->
                        L = erlang:length(Item#edis_item.value),
                        StartPos =
                          case Start of
                            Start when Start >= L -> throw(empty);
                            Start when Start >= 0 -> Start + 1;
                            Start when Start < (-1)*L -> 1;
                            Start -> L + 1 + Start
                          end,
                        StopPos =
                          case Stop of
                            Stop when Stop >= 0, Stop >= L -> L;
                            Stop when Stop >= 0 -> Stop + 1;
                            Stop when Stop < (-1)*L -> 0;
                            Stop -> L + 1 + Stop
                          end,
                        case StopPos - StartPos + 1 of
                          Len when Len =< 0 -> throw(empty);
                          Len ->
                            {ok, Item#edis_item{value =
                                                  lists:sublist(Item#edis_item.value, StartPos, Len)}}
                        end
                end) of
      {ok, ok} -> ok;
      {error, empty} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        ok;
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({rpop, Key}, _From, State) ->
  Reply =
    case update(State#state.db, Key, list,
                fun(Item) ->
                        case lists:reverse(Item#edis_item.value) of
                          [Value] ->
                            {{delete, Value}, Item#edis_item{value = []}};
                          [Value|Rest] ->
                            {{keep, Value}, Item#edis_item{value = lists:reverse(Rest)}};
                          [] ->
                            throw(not_found)
                        end
                end) of
      {ok, {delete, Value}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Value};
      {ok, {keep, Value}} ->
        {ok, Value};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({rpop_lpush, Key, Key}, _From, State) ->
  Reply =
    update(State#state.db, Key, list,
           fun(Item) ->
                   case lists:reverse(Item#edis_item.value) of
                     [Value|Rest] ->
                       {Value,
                        Item#edis_item{value = [Value | lists:reverse(Rest)]}};
                     _ ->
                       throw(not_found)
                   end
           end),
  {reply, Reply, stamp(Key, State)};
handle_call({rpop_lpush, Source, Destination}, _From, State) ->
  Reply =
    case update(State#state.db, Source, list,
                fun(Item) ->
                        case lists:reverse(Item#edis_item.value) of
                          [Value] ->
                            {{delete, Value}, Item#edis_item{value = []}};
                          [Value|Rest] ->
                            {{keep, Value}, Item#edis_item{value = lists:reverse(Rest)}};
                          [] ->
                            throw(not_found)
                        end
                end) of
      {ok, {delete, Value}} ->
        _ = eleveldb:delete(State#state.db, Source, []),
        update(State#state.db, Destination, list, linkedlist,
                fun(Item) ->
                        {Value, Item#edis_item{value = [Value | Item#edis_item.value]}}
                end, []);
      {ok, {keep, Value}} ->
        update(State#state.db, Destination, list, linkedlist,
                fun(Item) ->
                        {Value, Item#edis_item{value = [Value | Item#edis_item.value]}}
                end, []);
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp([Destination, Source], State)};
handle_call({rpush, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, list, linkedlist,
           fun(Item) ->
                   {length(Item#edis_item.value) + 1,
                    Item#edis_item{value =
                                     lists:reverse(
                                       [Value|
                                          lists:reverse(Item#edis_item.value)])}}
           end, []),
  {reply, Reply, stamp(Key, State)};
handle_call({rpush_x, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, list,
           fun(Item) ->
                   {length(Item#edis_item.value) + 1,
                    Item#edis_item{value =
                                     lists:reverse(
                                       [Value|
                                          lists:reverse(Item#edis_item.value)])}}
           end),
  {reply, Reply, stamp(Key, State)};
handle_call({sadd, Key, Members}, _From, State) ->
  Reply =
    update(State#state.db, Key, set, hashtable,
           fun(Item) ->
                   NewValue =
                     lists:foldl(fun sets:add_element/2, Item#edis_item.value, Members),
                   {sets:size(NewValue) - sets:size(Item#edis_item.value),
                    Item#edis_item{value = NewValue}}
           end, sets:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({scard, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} -> {ok, sets:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};

handle_call(X, _From, State) ->
  {stop, {unexpected_request, X}, {unexpected_request, X}, State}.

%% @hidden
-spec handle_cast(X, state()) -> {stop, {unexpected_request, X}, state()}.
handle_cast(X, State) -> {stop, {unexpected_request, X}, State}.

%% @hidden
-spec handle_info(term(), state()) -> {noreply, state(), hibernate}.
handle_info(_, State) -> {noreply, State, hibernate}.

%% @hidden
-spec terminate(term(), state()) -> ok.
terminate(_, _) -> ok.

%% @hidden
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% =================================================================================================
%% Private functions
%% =================================================================================================
%% @private
stamp(undefined, State) -> State;
stamp([], State) -> State;
stamp([Key|Keys], State) ->
  stamp(Keys, stamp(Key, State));
stamp(Key, State) ->
  State#state{accesses =
                dict:store(Key, edis_util:now() -
                             State#state.start_time, State#state.accesses)}.

%% @private
block_list_op(Key, Req, From, Timeout, State) ->
  CurrentList =
      case orddict:find(Key, State#state.blocked_list_ops) of
        error -> [];
        {ok, L} -> L
      end,
  State#state{blocked_list_ops =
                  orddict:store(Key,
                                lists:reverse([{Timeout, Req, From}|
                                                   lists:reverse(CurrentList)]),
                                State#state.blocked_list_ops)}.

%% @private
unblock_list_ops(Key, From, State) ->
  case orddict:find(Key, State#state.blocked_list_ops) of
    error -> State;
    {ok, List} ->
      State#state{blocked_list_ops =
                      orddict:store(
                        Key,
                        lists:filter(
                          fun({_, _, OpFrom}) ->
                                  OpFrom =/= From
                          end, List),
                        State#state.blocked_list_ops)
                      }
  end.

check_blocked_list_ops(Key, State) ->
  Now = edis_util:now(),
  case orddict:find(Key, State#state.blocked_list_ops) of
    error -> State;
    {ok, [{Timeout, Req, From = {To, _Tag}}]} when Timeout > Now ->
      case rpc:pinfo(To) of
        undefined -> %% Caller disconnected
          State#state{blocked_list_ops = orddict:erase(Key, State#state.blocked_list_ops)};
        _ ->
          case handle_call(Req, From, State) of
            {reply, {error, not_found}, NewState} -> %% still have to wait
              NewState;
            {reply, Reply, NewState} ->
              gen_server:reply(From, Reply),
              NewState#state{blocked_list_ops = orddict:erase(Key, NewState#state.blocked_list_ops)}
          end
      end;
    {ok, [{Timeout, Req, From = {To, _Tag}} | Rest]} when Timeout > Now ->
      case rpc:pinfo(To) of
        undefined -> %% Caller disconnected
          check_blocked_list_ops(
            Key, State#state{blocked_list_ops =
                                 orddict:store(Key, Rest, State#state.blocked_list_ops)});
        _ ->
          case handle_call(Req, From, State) of
            {reply, {error, not_found}, NewState} -> %% still have to wait
              NewState;
            {reply, Reply, NewState} ->
              gen_server:reply(From, Reply),
              check_blocked_list_ops(
                Key, NewState#state{blocked_list_ops =
                                        orddict:store(Key, Rest, NewState#state.blocked_list_ops)})
          end
      end;
    {ok, [_TimedOut]} ->
      State#state{blocked_list_ops = orddict:erase(Key, State#state.blocked_list_ops)};
    {ok, [_TimedOut|Rest]} ->
      check_blocked_list_ops(
            Key, State#state{blocked_list_ops =
                                 orddict:store(Key, Rest, State#state.blocked_list_ops)});
    {ok, []} ->
      State#state{blocked_list_ops = orddict:erase(Key, State#state.blocked_list_ops)}
  end.

first_that_works([], _From, State) ->
  {reply, {error, not_found}, State};
first_that_works([Req|Reqs], From, State) ->
  case handle_call(Req, From, State) of
    {reply, {error, not_found}, NewState} ->
      first_that_works(Reqs, From, NewState);
    OtherReply ->
      OtherReply
  end.

%% @private
exists_item(Db, Key) ->
  case eleveldb:get(Db, Key, []) of
    {ok, _} -> true;
    not_found -> false;
    {error, Reason} -> {error, Reason}
  end.

%% @private
get_item(Db, Type, Key) ->
  case eleveldb:get(Db, Key, []) of
    {ok, Bin} ->
      Now = edis_util:now(),
      case erlang:binary_to_term(Bin) of
        Item = #edis_item{type = T, expire = Expire}
          when Type =:= any orelse T =:= Type ->
          case Expire of
            Expire when Expire >= Now ->
              Item;
            _ ->
              _ = eleveldb:delete(Db, Key, []),
              not_found
          end;
        _Other -> {error, bad_item_type}
      end;
    not_found ->
      not_found;
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
update(Db, Key, Type, Fun) ->
  try
    {Res, NewItem} =
      case get_item(Db, Type, Key) of
        not_found ->
          throw(not_found);
        {error, Reason} ->
          throw(Reason);
        Item ->
          Fun(Item)
      end,
    case eleveldb:put(Db, Key, erlang:term_to_binary(NewItem), []) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:Error ->
      {error, Error}
  end.

%% @private
update(Db, Key, Type, Encoding, Fun, Default) ->
  try
    {Res, NewItem} =
      case get_item(Db, Type, Key) of
        not_found ->
          Fun(#edis_item{key = Key, type = Type, encoding = Encoding, value = Default});
        {error, Reason} ->
          throw(Reason);
        Item ->
          Fun(Item)
      end,
    case eleveldb:put(Db, Key, erlang:term_to_binary(NewItem), []) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:Error ->
      {error, Error}
  end.

%% @private
key_at(Db, 0) ->
  try
    Now = edis_util:now(),
    eleveldb:fold(
      Db, fun({_Key, Bin}, Acc) ->
                  case erlang:binary_to_term(Bin) of
                    #edis_item{key = Key, expire = Expire} when Expire >= Now ->
                      throw({ok, Key});
                    _ ->
                      Acc
                  end
      end, {ok, undefined}, [{fill_cache, false}])
  catch
    _:{ok, Key} -> {ok, Key}
  end;
key_at(Db, Index) when Index > 0 ->
  try
    Now = edis_util:now(),
    NextIndex =
      eleveldb:fold(
        Db, fun({_Key, Bin}, 0) ->
                    case erlang:binary_to_term(Bin) of
                      #edis_item{key = Key, expire = Expire} when Expire >= Now ->
                        throw({ok, Key});
                      _ ->
                        0
                    end;
               (_, AccIndex) ->
                    AccIndex - 1
        end, Index, [{fill_cache, false}]),
    key_at(Db, NextIndex)
  catch
    _:{ok, Key} -> {ok, Key}
  end.

%% @private
make_call(Process, Request) ->
  make_call(Process, Request, ?DEFAULT_TIMEOUT).

%% @private
make_call(Process, Request, Timeout) ->
  ?DEBUG("CALL for ~p: ~p~n", [Process, Request]),
  ok = edis_db_monitor:notify(Process, Request),
  try gen_server:call(Process, Request, Timeout) of
    ok -> ok;
    {ok, Reply} -> Reply;
    {error, Error} ->
      ?THROW("Error trying ~p on ~p:~n\t~p~n", [Request, Process, Error]),
      throw(Error)
  catch
    _:{timeout, _} ->
      throw(timeout)
  end.

timeout_to_seconds(infinity) -> infinity;
timeout_to_seconds(Timeout) -> edis_util:now() + Timeout.

timeout_to_ms(infinity) -> infinity;
timeout_to_ms(Timeout) -> Timeout * 1000.