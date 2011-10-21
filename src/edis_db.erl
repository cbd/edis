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
-export([run/2, run/3]).

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
%% @equiv run(Db, Command, ?DEFAULT_TIMEOUT)
-spec run(atom(), edis:command()) -> term().
run(Db, Command) ->
  run(Db, Command, ?DEFAULT_TIMEOUT).

-spec run(atom(), edis:command(), infinity | pos_integer()) -> term().
run(Db, Command, Timeout) ->
  ?DEBUG("CALL for ~p: ~p~n", [Db, Command]),
  try gen_server:call(Db, Command, Timeout) of
    ok -> ok;
    {ok, Reply} -> Reply;
    {error, Error} ->
      ?THROW("Error trying ~p on ~p:~n\t~p~n", [Command, Db, Error]),
      throw(Error)
  catch
    _:{timeout, _} ->
      throw(timeout)
  end.

%% =================================================================================================
%% Server functions
%% =================================================================================================
%% @hidden
-spec init(non_neg_integer()) -> {ok, state()} | {stop, any()}.
init(Index) ->
  _ = random:seed(erlang:now()),
  case eleveldb:open(edis_config:get(dir) ++ "/edis-" ++ integer_to_list(Index), [{create_if_missing, true}]) of
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
  {reply, {ok, <<"PONG">>}, State};
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
  ok = eleveldb:destroy(edis_config:get(dir) ++ "/edis-" ++ integer_to_list(State#state.index), []),
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
        try run(NewDb, #edis_command{cmd = <<"-INTERNAL-RECV">>, args = [Item]}) of
          ok ->
            case eleveldb:delete(State#state.db, Key, []) of
              ok ->
                {ok, true};
              {error, Reason} ->
                _ = run(NewDb, #edis_command{cmd = <<"DEL">>, args = [Key]}),
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
      not_found -> {ok, [undefined || _ <- Fields]};
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
              {ok, lists:nth((-1)*Index, lists:reverse(Value))}
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
handle_call({lpush, Key, Values}, From, State) ->
  Reply =
    update(State#state.db, Key, list, linkedlist,
           fun(Item) ->
                   {length(Item#edis_item.value) + length(Values),
                    Item#edis_item{value =
                                     lists:append(lists:reverse(Values), Item#edis_item.value)}}
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
handle_call({rpush, Key, Values}, _From, State) ->
  Reply =
    update(State#state.db, Key, list, linkedlist,
           fun(Item) ->
                   {length(Item#edis_item.value) + length(Values),
                    Item#edis_item{value = lists:append(Item#edis_item.value, Values)}}
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
                     lists:foldl(fun gb_sets:add_element/2, Item#edis_item.value, Members),
                   {gb_sets:size(NewValue) - gb_sets:size(Item#edis_item.value),
                    Item#edis_item{value = NewValue}}
           end, gb_sets:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({scard, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({sdiff, [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:to_list(Value)};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({sdiff, [Key | Keys]}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} ->
        {ok, gb_sets:to_list(
           lists:foldl(
             fun(SKey, AccSet) ->
                     case get_item(State#state.db, set, SKey) of
                       #edis_item{value = SValue} -> gb_sets:subtract(AccSet, SValue);
                       not_found -> AccSet;
                       {error, Reason} -> throw(Reason)
                     end
             end, Value, Keys))};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({sdiff_store, Destination, Keys}, From, State) ->
    case handle_call({sdiff, Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = eleveldb:delete(State#state.db, Destination, []),
        {reply, {ok, 0}, stamp([Destination|Keys], NewState)};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
          case eleveldb:put(State#state.db,
                            Destination,
                            erlang:term_to_binary(
                              #edis_item{key = Destination, type = set, encoding = hashtable,
                                         value = Value}), []) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp(Destination, NewState)};
      ErrorReply ->
        ErrorReply
    end;
handle_call({sinter, Keys}, _From, State) ->
  Reply =
    try gb_sets:intersection(
          [case get_item(State#state.db, set, Key) of
             #edis_item{value = Value} -> Value;
             not_found -> throw(empty);
             {error, Reason} -> throw(Reason)
           end || Key <- Keys]) of
      Set -> {ok, gb_sets:to_list(Set)}
    catch
      _:empty -> {ok, []};
      _:Error -> {error, Error}
    end,
  {reply, Reply, stamp(Keys, State)};
handle_call({sinter_store, Destination, Keys}, From, State) ->
    case handle_call({sinter, Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = eleveldb:delete(State#state.db, Destination, []),
        {reply, {ok, 0}, stamp([Destination|Keys], NewState)};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
          case eleveldb:put(State#state.db,
                            Destination,
                            erlang:term_to_binary(
                              #edis_item{key = Destination, type = set, encoding = hashtable,
                                         value = Value}), []) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp([Destination|Keys], NewState)};
      ErrorReply ->
        ErrorReply
    end;
handle_call({sismember, Key, Member}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:is_element(Member, Value)};
      not_found -> {ok, false};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({smembers, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:to_list(Value)};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({smove, Source, Destination, Member}, _From, State) ->
  Reply =
    case update(State#state.db, Source, set,
                fun(Item) ->
                        case gb_sets:is_element(Member, Item#edis_item.value) of
                          false ->
                            {false, Item};
                          true ->
                            NewValue = gb_sets:del_element(Member, Item#edis_item.value),
                            case gb_sets:size(NewValue) of
                              0 -> {delete, Item#edis_item{value = NewValue}};
                              _ -> {true, Item#edis_item{value = NewValue}}
                            end
                        end
                end) of
      {ok, delete} ->
        _ = eleveldb:delete(State#state.db, Source, []),
        update(State#state.db, Destination, set, hashtable,
               fun(Item) ->
                       {true, Item#edis_item{value =
                                               gb_sets:add_element(Member, Item#edis_item.value)}}
               end, gb_sets:empty());
      {ok, true} ->
        update(State#state.db, Destination, set, hashtable,
               fun(Item) ->
                       {true, Item#edis_item{value =
                                               gb_sets:add_element(Member, Item#edis_item.value)}}
               end, gb_sets:empty());
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp([Source, Destination], State)};
handle_call({spop, Key}, _From, State) ->
  Reply =
    case update(State#state.db, Key, set,
                fun(Item) ->
                        {Member, NewValue} = gb_sets:take_smallest(Item#edis_item.value),
                        case gb_sets:size(NewValue) of
                          0 -> {{delete, Member}, Item#edis_item{value = NewValue}};
                          _ -> {Member, Item#edis_item{value = NewValue}}
                        end
                end) of
      {ok, {delete, Member}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Member};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({srand_member, Key}, _From, State) ->
  _ = random:seed(erlang:now()),
  Reply =
    case get_item(State#state.db, set, Key) of
      #edis_item{value = Value} ->
        Iterator = gb_sets:iterator(Value),
        {Res, _} =
          lists:foldl(
           fun(_, {_, AccIterator}) ->
                   gb_sets:next(AccIterator)
           end, {undefined, Iterator},
           lists:seq(1, random:uniform(gb_sets:size(Value)))),
        {ok, Res};
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({srem, Key, Members}, _From, State) ->
  Reply =
    case update(State#state.db, Key, set,
                fun(Item) ->
                        NewValue =
                          lists:foldl(fun gb_sets:del_element/2, Item#edis_item.value, Members),
                        case gb_sets:size(NewValue) of
                          0 ->
                            {{delete, gb_sets:size(Item#edis_item.value)},
                             Item#edis_item{value = NewValue}};
                          N ->
                            {gb_sets:size(Item#edis_item.value) - N,
                             Item#edis_item{value = NewValue}}
                        end
                end) of
      {ok, {delete, Count}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Count};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({sunion, Keys}, _From, State) ->
  Reply =
    try gb_sets:union(
          [case get_item(State#state.db, set, Key) of
             #edis_item{value = Value} -> Value;
             not_found -> gb_sets:empty();
             {error, Reason} -> throw(Reason)
           end || Key <- Keys]) of
      Set -> {ok, gb_sets:to_list(Set)}
    catch
      _:empty -> {ok, []};
      _:Error -> {error, Error}
    end,
  {reply, Reply, stamp(Keys, State)};
handle_call({sunion_store, Destination, Keys}, From, State) ->
    case handle_call({sunion, Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = eleveldb:delete(State#state.db, Destination, []),
        {reply, {ok, 0}, stamp([Destination|Keys], NewState)};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
          case eleveldb:put(State#state.db,
                            Destination,
                            erlang:term_to_binary(
                              #edis_item{key = Destination, type = set, encoding = hashtable,
                                         value = Value}), []) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp([Destination|Keys], NewState)};
      ErrorReply ->
        ErrorReply
    end;
handle_call({zadd, Key, SMs}, _From, State) ->
  Reply =
    update(State#state.db, Key, zset, skiplist,
           fun(Item) ->
                   NewValue =
                     lists:foldl(fun zsets:enter/2, Item#edis_item.value, SMs),
                   {zsets:size(NewValue) - zsets:size(Item#edis_item.value),
                    Item#edis_item{value = NewValue}}
           end, zsets:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({zcard, Key}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zcount, Key, Min, Max}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:count(Min, Max, Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zincr, Key, Increment, Member}, _From, State) ->
  Reply =
    update(State#state.db, Key, zset, skiplist,
           fun(Item) ->
                   NewScore =
                     case zsets:find(Member, Item#edis_item.value) of
                       error -> Increment;
                       {ok, Score} -> Score + Increment
                     end,
                   {NewScore, 
                    Item#edis_item{value = zsets:enter(NewScore, Member, Item#edis_item.value)}}
           end, zsets:new()),
  {reply, Reply, stamp(Key, State)};
handle_call({zinter_store, Destination, WeightedKeys, Aggregate}, _From, State) ->
  Reply =
    try weighted_intersection(
          Aggregate,
          [case get_item(State#state.db, zset, Key) of
              #edis_item{value = Value} -> {Value, Weight};
              not_found -> throw(empty);
              {error, Reason} -> throw(Reason)
            end || {Key, Weight} <- WeightedKeys]) of
      ZSet ->
        case zsets:size(ZSet) of
          0 ->
            _ = eleveldb:delete(State#state.db, Destination, []),
            {ok, 0};
          Size ->
            case eleveldb:put(State#state.db,
                              Destination,
                              erlang:term_to_binary(
                                #edis_item{key = Destination, type = zset, encoding = skiplist,
                                           value = ZSet}), []) of
              ok -> {ok, Size};
              {error, Reason} -> {error, Reason}
            end
        end
    catch
      _:empty ->
        _ = eleveldb:delete(State#state.db, Destination, []),
        {ok, 0};
      _:Error ->
        ?ERROR("~p~n", [Error]),
        {error, Error}
    end,
  {reply, Reply, stamp([Destination|[Key || {Key, _} <- WeightedKeys]], State)};
handle_call({zrange, Key, Start, Stop}, _From, State) ->
  Reply =
    try
      case get_item(State#state.db, zset, Key) of
        #edis_item{value = Value} ->
          L = zsets:size(Value),
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
          case StopPos of
            StopPos when StopPos < StartPos -> {ok, []};
            StopPos -> {ok, zsets:range(StartPos, StopPos, Value)}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrange_by_score, Key, Min, Max}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:list(Min, Max, Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrank, Key, Member}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, zsets:count(neg_infinity, {exc, Score}, Value)}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrem, Key, Members}, _From, State) ->
  Reply =
    case update(State#state.db, Key, zset,
                fun(Item) ->
                        NewValue =
                          lists:foldl(fun zsets:delete_any/2, Item#edis_item.value, Members),
                        case zsets:size(NewValue) of
                          0 ->
                            {{delete, zsets:size(Item#edis_item.value)},
                             Item#edis_item{value = NewValue}};
                          N ->
                            {zsets:size(Item#edis_item.value) - N,
                             Item#edis_item{value = NewValue}}
                        end
                end) of
      {ok, {delete, Count}} ->
        _ = eleveldb:delete(State#state.db, Key, []),
        {ok, Count};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrem_range_by_rank, Key, Start, Stop}, From, State) ->
  case handle_call({zrange, Key, Start, Stop}, From, State) of
    {reply, {ok, SMs}, NewState} ->
      handle_call({zrem, Key, [Member || {_Score, Member} <- SMs]}, From, NewState);
    OtherReply ->
      OtherReply
  end;
handle_call({zrem_range_by_score, Key, Min, Max}, From, State) ->
  case handle_call({zrange_by_score, Key, Min, Max}, From, State) of
    {reply, {ok, SMs}, NewState} ->
      handle_call({zrem, Key, [Member || {_Score, Member} <- SMs]}, From, NewState);
    OtherReply ->
      OtherReply
  end;
handle_call({zrev_range, Key, Start, Stop}, _From, State) ->
  Reply =
    try
      case get_item(State#state.db, zset, Key) of
        #edis_item{value = Value} ->
          L = zsets:size(Value),
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
          case StopPos of
            StopPos when StopPos < StartPos -> {ok, []};
            StopPos -> {ok, zsets:range(StartPos, StopPos, Value, backwards)}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrev_range_by_score, Key, Min, Max}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:list(Min, Max, Value, backwards)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zrev_rank, Key, Member}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, zsets:count(infinity, {exc, Score}, Value, backwards)}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zscore, Key, Member}, _From, State) ->
  Reply =
    case get_item(State#state.db, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, Score}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, State)};
handle_call({zunion_store, Destination, WeightedKeys, Aggregate}, _From, State) ->
  Reply =
    try weighted_union(
          Aggregate,
          [case get_item(State#state.db, zset, Key) of
              #edis_item{value = Value} -> {Value, Weight};
              not_found -> {zsets:new(), 0.0};
              {error, Reason} -> throw(Reason)
            end || {Key, Weight} <- WeightedKeys]) of
      ZSet ->
        case zsets:size(ZSet) of
          0 ->
            _ = eleveldb:delete(State#state.db, Destination, []),
            {ok, 0};
          Size ->
            case eleveldb:put(State#state.db,
                              Destination,
                              erlang:term_to_binary(
                                #edis_item{key = Destination, type = zset, encoding = skiplist,
                                           value = ZSet}), []) of
              ok -> {ok, Size};
              {error, Reason} -> {error, Reason}
            end
        end
    catch
      _:empty ->
        _ = eleveldb:delete(State#state.db, Destination, []),
        {ok, 0};
      _:Error ->
        ?ERROR("~p~n", [Error]),
        {error, Error}
    end,
  {reply, Reply, stamp([Destination|[Key || {Key, _} <- WeightedKeys]], State)};

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

weighted_intersection(_Aggregate, [{ZSet, Weight}]) ->
  zsets:map(fun(Score, _) -> Score * Weight end, ZSet);
weighted_intersection(Aggregate, [{ZSet, Weight}|WeightedZSets]) ->
  weighted_intersection(Aggregate, WeightedZSets, Weight, ZSet).

weighted_intersection(_Aggregate, [], 1.0, AccZSet) -> AccZSet;
weighted_intersection(Aggregate, [{ZSet, Weight} | Rest], AccWeight, AccZSet) ->
  weighted_intersection(
    Aggregate, Rest, 1.0,
    zsets:intersection(
      fun(Score, AccScore) ->
              lists:Aggregate([Score * Weight, AccScore * AccWeight])
      end, ZSet, AccZSet)).

weighted_union(_Aggregate, [{ZSet, Weight}]) ->
  zsets:map(fun(Score, _) -> Score * Weight end, ZSet);
weighted_union(Aggregate, [{ZSet, Weight}|WeightedZSets]) ->
  weighted_union(Aggregate, WeightedZSets, Weight, ZSet).

weighted_union(_Aggregate, [], 1.0, AccZSet) -> AccZSet;
weighted_union(Aggregate, [{ZSet, Weight} | Rest], AccWeight, AccZSet) ->
  weighted_union(
    Aggregate, Rest, 1.0,
    zsets:union(
      fun(undefined, AccScore) ->
              AccScore * AccWeight;
         (Score, undefined) ->
              Score * Weight;
         (Score, AccScore) ->
              lists:Aggregate([Score * Weight, AccScore * AccWeight])
      end, ZSet, AccZSet)).
