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
                backend_mod         :: atom(),
                backend_ref         :: term(),
                start_time          :: pos_integer(),
                accesses            :: dict(),
                updates             :: dict(),
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
%% @doc starts a new db client
start_link(Index) ->
  gen_server:start_link({local, process(Index)}, ?MODULE, Index, []).

%% @doc returns the database name with index Index 
%% You can use that value later on calls to {@link run/2} or {@link run/3}
-spec process(non_neg_integer()) -> atom().
process(Index) ->
  list_to_atom("edis-db-" ++ integer_to_list(Index)).

%% =================================================================================================
%% Commands
%% =================================================================================================
%% @equiv run(Db, Command, 5000)
-spec run(atom(), edis:command()) -> term().
run(Db, Command) ->
  run(Db, Command, ?DEFAULT_TIMEOUT).

%% @doc Executes Command in Db with some Timeout 
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
  {Mod, InitArgs} = edis_config:get(backend),
  case Mod:init(edis_config:get(dir), Index, InitArgs) of
    {ok, Ref} ->
      {ok, #state{index = Index, backend_mod = Mod, backend_ref = Ref,
                  last_save = edis_util:timestamp(), start_time = edis_util:now(),
                  accesses = dict:new(), updates = dict:new(), blocked_list_ops = dict:new()}};
    {error, Reason} ->
      ?THROW("Couldn't start backend (~p): ~p~n", [{Mod, InitArgs}, Reason]),
      {stop, Reason}
  end.

%% @hidden
-spec handle_call(term(), reference(), state()) -> {reply, ok | {ok, term()} | {error, term()}, state()} | {stop, {unexpected_request, term()}, {unexpected_request, term()}, state()}.
handle_call(#edis_command{cmd = <<"PING">>}, _From, State) ->
  {reply, {ok, <<"PONG">>}, State};
handle_call(#edis_command{cmd = <<"ECHO">>, args = [Word]}, _From, State) ->
  {reply, {ok, Word}, State};
%% -- Strings --------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"APPEND">>, args = [Key, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   NewV = <<OldV/binary, Value/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}}
           end, <<>>),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"DECR">>, args = [Key]}, From, State) ->
  handle_call(#edis_command{cmd = <<"DECRBY">>, args = [Key, 1]}, From, State);
handle_call(#edis_command{cmd = <<"DECRBY">>, args = [Key, Decrement]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"GET">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
      #edis_item{type = string, value = Value} -> {ok, Value};
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"GETBIT">>, args = [Key, Offset]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
      #edis_item{value =
                   <<_:Offset/unit:1, Bit:1/unit:1, _Rest/bitstring>>} -> {ok, Bit};
      #edis_item{} -> {ok, 0}; %% Value is shorter than offset
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"GETRANGE">>, args = [Key, Start, End]}, _From, State) ->
  Reply =
    try
      case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
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
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"GETSET">>, args = [Key, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
           fun(Item = #edis_item{value = OldV}) ->
                   {OldV, Item#edis_item{value = Value}}
           end, undefined),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"INCR">>, args = [Key]}, From, State) ->
  handle_call(#edis_command{cmd = <<"INCRBY">>, args = [Key, 1]}, From, State);
handle_call(#edis_command{cmd = <<"INCRBY">>, args = [Key, Increment]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"MGET">>, args = Keys}, _From, State) ->
  Reply =
    lists:foldr(
      fun(Key, {ok, AccValues}) ->
              case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
                #edis_item{type = string, value = Value} -> {ok, [Value | AccValues]};
                not_found -> {ok, [undefined | AccValues]};
                {error, bad_item_type} -> {ok, [undefined | AccValues]};
                {error, Reason} -> {error, Reason}
              end;
         (_, AccErr) -> AccErr
      end, {ok, []}, Keys),
  {reply, Reply, stamp(Keys, read, State)};
handle_call(#edis_command{cmd = <<"MSET">>, args = KVs}, _From, State) ->
  Reply =
      (State#state.backend_mod):write(
        State#state.backend_ref,
        [{put, Key,
          #edis_item{key = Key, encoding = raw,
                     type = string, value = Value}} || {Key, Value} <- KVs]),
  {reply, Reply, stamp([K || {K, _} <- KVs], write, State)};
handle_call(#edis_command{cmd = <<"MSETNX">>, args = KVs}, _From, State) ->
  {Reply, Action} =
    case lists:any(
           fun({Key, _}) ->
                   exists_item(State#state.backend_mod, State#state.backend_ref, Key)
           end, KVs) of
      true ->
        {{ok, 0}, read};
      false ->
        ok = (State#state.backend_mod):write(
               State#state.backend_ref,
               [{put, Key,
                 #edis_item{key = Key, encoding = raw,
                            type = string, value = Value}} || {Key, Value} <- KVs]),
        {{ok, 1}, write}
    end,
  {reply, Reply, stamp([K || {K, _} <- KVs], Action, State)};
handle_call(#edis_command{cmd = <<"SET">>, args = [Key, Value]}, From, State) ->
  handle_call(#edis_command{cmd = <<"MSET">>, args = [{Key, Value}]}, From, State);
handle_call(#edis_command{cmd = <<"SETBIT">>, args = [Key, Offset, Bit]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"SETEX">>, args = [Key, Seconds, Value]}, _From, State) ->
  Reply =
      (State#state.backend_mod):put(
        State#state.backend_ref, Key,
        #edis_item{key = Key, type = string, encoding = raw,
                   expire = edis_util:now() + Seconds,
                   value = Value}),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"SETNX">>, args = [Key, Value]}, From, State) ->
  handle_call(#edis_command{cmd = <<"MSETNX">>, args = [{Key, Value}]}, From, State);
handle_call(#edis_command{cmd = <<"SETRANGE">>, args = [Key, Offset, Value]}, _From, State) ->
  Length = erlang:size(Value),
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, string, raw,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"STRLEN">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
      #edis_item{value = Value} -> {ok, erlang:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
%% -- Keys -----------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"DEL">>, args = Keys}, _From, State) ->
  DeleteActions =
      [{delete, Key} || Key <- Keys, exists_item(State#state.backend_mod, State#state.backend_ref, Key)],
  Reply =
    case (State#state.backend_mod):write(State#state.backend_ref, DeleteActions) of
      ok -> {ok, length(DeleteActions)};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Keys, write, State)};
handle_call(#edis_command{cmd = <<"EXISTS">>, args = [Key]}, _From, State) ->
  Reply =
      case exists_item(State#state.backend_mod, State#state.backend_ref, Key) of
        true -> {ok, true};
        false -> {ok, false};
        {error, Reason} -> {error, Reason}
      end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"EXPIRE">>, args = [Key, Seconds]}, From, State) ->
  handle_call(#edis_command{cmd = <<"EXPIREAT">>, args = [Key, edis_util:now() + Seconds]}, From, State);
handle_call(#edis_command{cmd = <<"EXPIREAT">>, args = [Key, Timestamp]}, _From, State) ->
  Reply =
      case edis_util:now() of
        Now when Timestamp =< Now -> %% It's a delete (it already expired)
          case exists_item(State#state.backend_mod, State#state.backend_ref, Key) of
            true ->
              case (State#state.backend_mod):delete(State#state.backend_ref, Key) of
                ok -> {ok, true};
                {error, Reason} -> {error, Reason}
              end;
            false ->
              {ok, false}
          end;
        _ ->
          case update(State#state.backend_mod, State#state.backend_ref, Key, any,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"KEYS">>, args = [Pattern]}, _From, State) ->
  Reply =
    case re:compile(Pattern) of
      {ok, Compiled} ->
        Now = edis_util:now(),
        Keys =
            (State#state.backend_mod):fold(
                 State#state.backend_ref,
                 fun(Item, Acc) ->
                         case re:run(Item#edis_item.key, Compiled) of
                           nomatch ->
                             Acc;
                           _ ->
                             case Item#edis_item.expire of
                               Expire when Expire >= Now ->
                                 [Item#edis_item.key | Acc];
                               _ ->
                                 Acc
                             end
                         end
                 end, []),
        {ok, lists:reverse(Keys)};
      {error, {Reason, _Line}} when is_list(Reason) ->
        {error, "Invalid pattern: " ++ Reason};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"MOVE">>, args = [Key, NewDb]}, _From, State) ->
  {Reply, Action} =
    case get_item(State#state.backend_mod, State#state.backend_ref, string, Key) of
      not_found ->
        {{ok, false}, none};
      {error, Reason} ->
        {{error, Reason}, read};
      Item ->
        try run(NewDb, #edis_command{cmd = <<"-INTERNAL-RECV">>, args = [Item]}) of
          ok ->
            case (State#state.backend_mod):delete(State#state.backend_ref, Key) of
              ok ->
                {{ok, true}, write};
              {error, Reason} ->
                _ = run(NewDb, #edis_command{cmd = <<"DEL">>, args = [Key]}),
                {{error, Reason}, write}
            end
        catch
          _:found -> {{ok, false}, none};
          _:{error, Reason} -> {{error, Reason}, read}
        end
    end,
  {reply, Reply, stamp(Key, Action, State)};
handle_call(#edis_command{cmd = <<"-INTERNAL-RECV">>, args = [Item]}, _From, State) ->
  {Reply, Action} =
    case exists_item(State#state.backend_mod, State#state.backend_ref, Item#edis_item.key) of
      true -> {{error, found}, read};
      false -> {(State#state.backend_mod):put(
                  State#state.backend_ref, Item#edis_item.key, Item), write}
    end,
  {reply, Reply, stamp(Item#edis_item.key, Action, State)};
handle_call(#edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [Key]}, _From, State) ->
  Reply =
    case exists_item(State#state.backend_mod, State#state.backend_ref, Key) of
      true -> {ok, 1};
      false -> {ok, 0}
    end,
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"OBJECT ENCODING">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, any, Key) of
      #edis_item{encoding = Encoding} -> {ok, atom_to_binary(Encoding, utf8)};
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"OBJECT IDLETIME">>, args = [Key]}, _From, State) ->
  Reply =
    case exists_item(State#state.backend_mod, State#state.backend_ref, Key) of
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
handle_call(#edis_command{cmd = <<"OBJECT LASTUPDATE">>, args = [Key]}, _From, State) ->
  Reply =
      case dict:find(Key, State#state.updates) of
        {ok, Offset} -> {ok, Offset + State#state.start_time};
        error -> {ok, undefined}
      end,
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"PERSIST">>, args = [Key]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, any,
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
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"RANDOMKEY">>}, _From, State) ->
  Reply =
    case (State#state.backend_mod):is_empty(State#state.backend_ref) of
      true -> {ok, undefined};
      false ->
        %%TODO: Make it really random... not just on the first xx tops
        %%      BUT we need to keep it O(1)
        RandomIndex = random:uniform(?RANDOM_THRESHOLD),
        key_at(State#state.backend_mod, State#state.backend_ref, RandomIndex)
    end,
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"RENAME">>, args = [Key, NewKey]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, any, Key) of
      not_found ->
        {error, no_such_key};
      {error, Reason} ->
        {error, Reason};
      Item ->
        (State#state.backend_mod):write(State#state.backend_ref,
                                        [{delete, Key},
                                         {put, NewKey, Item#edis_item{key = NewKey}}])
    end,
  {reply, Reply, stamp([Key, NewKey], write, State)};
handle_call(#edis_command{cmd = <<"RENAMENX">>, args = [Key, NewKey]}, _From, State) ->
  {Reply, Action} =
    case get_item(State#state.backend_mod, State#state.backend_ref, any, Key) of
      not_found ->
        {{error, no_such_key}, none};
      {error, Reason} ->
        {{error, Reason}, read};
      Item ->
        case exists_item(State#state.backend_mod, State#state.backend_ref, NewKey) of
          true ->
            {{ok, false}, read};
          false ->
            ok = (State#state.backend_mod):write(State#state.backend_ref,
                                                 [{delete, Key},
                                                  {put, NewKey, Item#edis_item{key = NewKey}}]),
            {{ok, true}, write}
        end
    end,
  {reply, Reply, stamp([Key, NewKey], Action, State)};
handle_call(#edis_command{cmd = <<"TTL">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, any, Key) of
      not_found ->
        {ok, -1};
      #edis_item{expire = infinity} ->
        {ok, -1};
      Item ->
        {ok, Item#edis_item.expire - edis_util:now()}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"TYPE">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, any, Key) of
      not_found ->
        {ok, <<"none">>};
      Item ->
        {ok, atom_to_binary(Item#edis_item.type, utf8)}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SORT">>, args = [Key, Options = #edis_sort_options{store_in = undefined}]}, _From, State) ->
  Reply =
      case get_item(State#state.backend_mod, State#state.backend_ref, [list, set, zset], Key) of
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason};
        Item -> sort(State#state.backend_mod, State#state.backend_ref, Item, Options)
      end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(C = #edis_command{cmd = <<"SORT">>, args = [Key, Options = #edis_sort_options{store_in = Destination}]}, From, State) ->
  case handle_call(C#edis_command{args = [Key, Options#edis_sort_options{store_in = undefined}]}, From, State) of
    {reply, {ok, []}, NewState} ->
      {reply, {ok, []}, NewState};
    {reply, {ok, Sorted}, NewState} ->
      Reply =
          case (State#state.backend_mod):put(
                 NewState#state.backend_ref, Destination,
                 #edis_item{key = Destination, type = list, encoding = linkedlist, value = Sorted}) of
            ok -> {ok, erlang:length(Sorted)};
            {error, Reason} -> {error, Reason}
          end,
      {reply, Reply, stamp(Destination, write, NewState)};
    OtherReply ->
      OtherReply
  end;

%% -- Hashes ---------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"HDEL">>, args = [Key | Fields]}, _From, State) ->
  {Reply, Action} =
    case update(State#state.backend_mod, State#state.backend_ref, Key, hash,
                fun(Item) ->
                        NewDict = lists:foldl(fun dict:erase/2, Item#edis_item.value, Fields),
                        {{dict:size(Item#edis_item.value) - dict:size(NewDict),
                          dict:size(NewDict)},
                         Item#edis_item{value = NewDict}}
                end) of
      {ok, {Deleted, 0}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {{ok, Deleted}, write};
      {ok, {Deleted, _}} ->
        {{ok, Deleted}, write};
      {error, not_found} ->
        {{ok, 0}, none};
      {error, Reason} ->
        {{error, Reason}, read}
    end,
  {reply, Reply, stamp(Key, Action, State)};
handle_call(#edis_command{cmd = <<"HEXISTS">>, args = [Key, Field]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, false};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:is_key(Field, Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HGET">>, args = [Key, Field]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason};
      Item -> {ok, case dict:find(Field, Item#edis_item.value) of
                     {ok, Value} -> Value;
                     error -> undefined
                   end}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HGETALL">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, lists:flatmap(fun tuple_to_list/1, dict:to_list(Item#edis_item.value))}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HINCRBY">>, args = [Key, Field, Increment]}, _From, State) ->
  Reply =
    update(
      State#state.backend_mod, State#state.backend_ref, Key, hash, hashtable,
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
                    _:not_integer ->
                      throw({not_integer, "hash value"})
                  end
              end
      end, dict:new()),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"HKEYS">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:fetch_keys(Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HLEN">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:size(Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HMGET">>, args = [Key | Fields]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
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
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"HMSET">>, args = [Key, FVs]}, _From, State) ->
  Reply =
    update(
      State#state.backend_mod, State#state.backend_ref, Key, hash, hashtable,
      fun(Item) ->
              lists:foldl(
                fun({Field, Value}, {AccStatus, AccItem}) ->
                        case dict:is_key(Field, Item#edis_item.value) of
                          true ->
                            {AccStatus,
                             Item#edis_item{value =
                                              dict:store(Field, Value, AccItem#edis_item.value)}};
                          false ->
                            {true,
                             Item#edis_item{value =
                                              dict:store(Field, Value, AccItem#edis_item.value)}}
                        end
                end, {false, Item}, FVs)
      end, dict:new()),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"HSET">>, args = [Key, Field, Value]}, From, State) ->
  handle_call(#edis_command{cmd = <<"HMSET">>, args = [Key, [{Field, Value}]]}, From, State);
handle_call(#edis_command{cmd = <<"HSETNX">>, args = [Key, Field, Value]}, _From, State) ->
  Reply =
    update(
      State#state.backend_mod, State#state.backend_ref, Key, hash, hashtable,
      fun(Item) ->
              case dict:is_key(Field, Item#edis_item.value) of
                true -> {false, Item};
                false -> {true, Item#edis_item{value =
                                                 dict:store(Field, Value, Item#edis_item.value)}}
              end
      end, dict:new()),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"HVALS">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, hash, Key) of
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason};
      Item -> {ok, dict:fold(fun(_,Value,Acc) ->
                                     [Value|Acc]
                             end, [], Item#edis_item.value)}
    end,
  {reply, Reply, stamp(Key, read, State)};
%% -- Lists ----------------------------------------------------------------------------------------
handle_call(C = #edis_command{cmd = <<"BLPOP">>, args = Keys, expire = Timeout}, From, State) ->
  Reqs = [C#edis_command{cmd = <<"-INTERNAL-BLPOP">>, args = [Key]} || Key <- Keys],
  case first_that_works(Reqs, From, State) of
    {reply, {error, not_found}, NewState} ->
      {noreply,
       lists:foldl(
         fun(Key, AccState) ->
                 block_list_op(Key,
                               C#edis_command{cmd = <<"-INTERNAL-BLPOP">>,
                                              args = [Key|Keys]}, From, Timeout, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call(C = #edis_command{cmd = <<"-INTERNAL-BLPOP">>, args = [Key | Keys]}, From, State) ->
  case handle_call(C#edis_command{cmd = <<"LPOP">>, args = [Key]}, From, State) of
    {reply, {ok, undefined}, NewState} ->
      {reply, {error, not_found}, NewState};
    {reply, {ok, Value}, NewState} ->
      {reply, {ok, [Key, Value]},
       lists:foldl(
         fun(K, AccState) ->
                 unblock_list_ops(K, From, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call(C = #edis_command{cmd = <<"BRPOP">>, args = Keys, expire = Timeout}, From, State) ->
  Reqs = [C#edis_command{cmd = <<"-INTERNAL-BRPOP">>, args = [Key]} || Key <- Keys],
  case first_that_works(Reqs, From, State) of
    {reply, {error, not_found}, NewState} ->
      {noreply,
       lists:foldl(
         fun(Key, AccState) ->
                 block_list_op(Key,
                               C#edis_command{cmd = <<"-INTERNAL-BRPOP">>,
                                              args = [Key|Keys]}, From, Timeout, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call(C = #edis_command{cmd = <<"-INTERNAL-BRPOP">>, args = [Key | Keys]}, From, State) ->
  case handle_call(C#edis_command{cmd = <<"RPOP">>, args = [Key]}, From, State) of
    {reply, {ok, undefined}, NewState} ->
      {reply, {error, not_found}, NewState};
    {reply, {ok, Value}, NewState} ->
      {reply, {ok, [Key, Value]},
       lists:foldl(
         fun(K, AccState) ->
                 unblock_list_ops(K, From, AccState)
         end, NewState, Keys)};
    OtherReply ->
      OtherReply
  end;
handle_call(C = #edis_command{cmd = <<"BRPOPLPUSH">>, args = [Source, Destination], expire = Timeout}, From, State) ->
  Req = C#edis_command{cmd = <<"RPOPLPUSH">>, args = [Source, Destination]},
  case handle_call(Req, From, State) of
    {reply, {ok, undefined}, NewState} ->
      {noreply, block_list_op(Source, Req, From, Timeout, NewState)};
    {reply, {error, not_found}, NewState} ->
      {noreply, block_list_op(Source, Req, From, Timeout, NewState)};
    OtherReply ->
      OtherReply
  end;
handle_call(#edis_command{cmd = <<"LINDEX">>, args = [Key, Index]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, list, Key) of
      #edis_item{value = Value} ->
        case Index of
          Index when Index >= 0 ->
            {ok, edis_lists:nth(Index + 1, Value)};
          Index ->
            {ok, edis_lists:nth((-1)*Index, edis_lists:reverse(Value))}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"LINSERT">>, args = [Key, Position, Pivot, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list,
           fun(Item) ->
                   List = Item#edis_item.value,
                   case edis_lists:insert(Value, Position, Pivot, List) of
                     List -> {-1, Item};
                     NewL -> {edis_lists:length(NewL), Item#edis_item{value = NewL}}
                   end
           end, -1),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"LLEN">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, list, Key) of
      #edis_item{value = Value} -> {ok, edis_lists:length(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"LPOP">>, args = [Key]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, list,
                fun(Item) ->
                        try
                          {Elem, NewV} = edis_lists:pop(Item#edis_item.value),
                          case edis_lists:length(NewV) of
                            0 -> {{delete, Elem}, Item#edis_item{value = NewV}};
                            _ -> {{keep, Elem}, Item#edis_item{value = NewV}}
                          end
                        catch
                          _:empty -> throw(not_found)
                        end
                end, {keep, undefined}) of
      {ok, {delete, Value}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {ok, Value};
      {ok, {keep, Value}} ->
        {ok, Value};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"LPUSH">>, args = [Key | Values]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list, linkedlist,
           fun(Item) ->
                   NewList = edis_lists:from_list(lists:reverse(Values)),
                   {edis_lists:length(Item#edis_item.value) + edis_lists:length(NewList),
                    Item#edis_item{value = edis_lists:append(NewList, Item#edis_item.value)}}
           end, edis_lists:empty()),
  NewState = check_blocked_list_ops(Key, State),
  {reply, Reply, stamp(Key, write, NewState)};
handle_call(#edis_command{cmd = <<"LPUSHX">>, args = [Key, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list,
           fun(Item) ->
                   {edis_lists:length(Item#edis_item.value) + 1,
                    Item#edis_item{value = edis_lists:push(Value, Item#edis_item.value)}}
           end, 0),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"LRANGE">>, args = [Key, Start, Stop]}, _From, State) ->
  Reply =
    try
      case get_item(State#state.backend_mod, State#state.backend_ref, list, Key) of
        #edis_item{value = Value} ->
          L = edis_lists:length(Value),
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
            Len -> {ok, edis_lists:to_list(edis_lists:sublist(Value, StartPos, Len))}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"LREM">>, args = [Key, Count, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list,
           fun(Item) ->
                   NewV =
                     case Count of
                       0 ->
                         edis_lists:filter(fun(Val) ->
                                                   Val =/= Value
                                           end, Item#edis_item.value);
                       Count when Count >= 0 ->
                         edis_lists:remove(Value, Count, Item#edis_item.value);
                       Count ->
                         edis_lists:reverse(
                           edis_lists:remove(Value, (-1)*Count,
                             edis_lists:reverse(Item#edis_item.value)))
                     end,
                   {edis_lists:length(Item#edis_item.value) - edis_lists:length(NewV),
                    Item#edis_item{value = NewV}}
           end, 0),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"LSET">>, args = [Key, Index, Value]}, _From, State) ->
  Reply =
    case
      update(State#state.backend_mod, State#state.backend_ref, Key, list,
             fun(Item) ->
                     case Index of
                       0 ->
                         {ok, Item#edis_item{value =
                                               edis_lists:replace_head(Value, Item#edis_item.value)}};
                       -1 ->
                         {ok, Item#edis_item{value =
                                               edis_lists:reverse(
                                                 edis_lists:replace_head(
												   Value, edis_lists:reverse(Item#edis_item.value)))}};
                       Index when Index >= 0 ->
                         {Before, After} = edis_lists:split(Index, Item#edis_item.value),
                         case edis_lists:length(After) of
                           0 -> throw(badarg);
                           _ ->
                             {ok, Item#edis_item{value =
                                                   edis_lists:append(
                                                     Before,
                                                     edis_lists:replace_head(Value, After))}}
                         end;
                       Index ->
                         {RAfter, RBefore} =
                           edis_lists:split((-1)*Index, edis_lists:reverse(Item#edis_item.value)),
                         {ok, Item#edis_item{value =
                                               edis_lists:append(
                                                 edis_lists:reverse(RBefore),
                                                 edis_lists:replace_head(
                                                   Value, edis_lists:reverse(RAfter)))}}
                     end
             end) of
      {ok, ok} -> ok;
      {error, not_found} -> {error, no_such_key};
      {error, badarg} -> {error, {out_of_range, "index"}};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"LTRIM">>, args = [Key, Start, Stop]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, list,
                fun(Item) ->
                        L = edis_lists:length(Item#edis_item.value),
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
                                                  edis_lists:sublist(
                                                    Item#edis_item.value, StartPos, Len)}}
                        end
                end, ok) of
      {ok, ok} -> ok;
      {error, empty} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        ok;
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"RPOP">>, args = [Key]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, list,
                fun(Item) ->
                        try
                          {Elem, NewV} = edis_lists:pop(edis_lists:reverse(Item#edis_item.value)),
                          case edis_lists:length(NewV) of
                            0 -> {{delete, Elem}, Item#edis_item{value = NewV}};
                            _ -> {{keep, Elem}, Item#edis_item{value = edis_lists:reverse(NewV)}}
                          end
                        catch
                          _:empty -> throw(not_found)
                        end
                end, {keep, undefined}) of
      {ok, {delete, Value}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {ok, Value};
      {ok, {keep, Value}} ->
        {ok, Value};
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"RPOPLPUSH">>, args = [Key, Key]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list,
           fun(Item) ->
                   try edis_lists:pop(edis_lists:reverse(Item#edis_item.value)) of
                     {Elem, NewV} ->
                       {Elem, Item#edis_item{value =
                                               edis_lists:push(Elem, edis_lists:reverse(NewV))}}
                   catch
                     _:empty -> throw(not_found)
                   end
           end, undefined),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"RPOPLPUSH">>, args = [Source, Destination]}, _From, State) ->
	Reply = 
		try
			{SourceAction,Value} = 
				case get_item(State#state.backend_mod, State#state.backend_ref, list, Source) of
					{error, SReason} -> throw(SReason);
					not_found -> throw(not_found);
					SourceItem ->
						{I,Rest} = edis_lists:pop(edis_lists:reverse(SourceItem#edis_item.value)),
						case edis_lists:length(Rest) of
							0 -> {{delete, Source},I};
							_ -> {{put, Source, SourceItem#edis_item{value = edis_lists:reverse(Rest)}},I}
						end
				end,
			DestinationAction = 
				case get_item(State#state.backend_mod, State#state.backend_ref, list, Destination) of
					{error, DReason} -> throw(DReason);
					not_found -> {put, Destination, #edis_item{key = Destination, type = list, encoding = hashtable, value = edis_lists:from_list([Value])}};
					DestinationItem -> {put, Destination, DestinationItem#edis_item{value = edis_lists:push(Value,DestinationItem#edis_item.value)}}
				end,
			case (State#state.backend_mod):write(State#state.backend_ref,
												 [SourceAction,DestinationAction]) of
				ok -> {ok, Value};
				{error, Reason} -> {error, Reason}
			end
		catch
			_:not_found -> {ok, undefined};
			_:Error -> 
				{error, Error}
		end,
  NewState = check_blocked_list_ops(Destination, State),
  {reply, Reply, stamp([Destination, Source], write, NewState)};
handle_call(#edis_command{cmd = <<"RPUSH">>, args = [Key | Values]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list, linkedlist,
           fun(Item) ->
                   NewList = edis_lists:from_list(Values),
                   {edis_lists:length(Item#edis_item.value) + edis_lists:length(NewList),
                    Item#edis_item{value = edis_lists:append(Item#edis_item.value, NewList)}}
           end, edis_lists:empty()),
  NewState = check_blocked_list_ops(Key, State),
  {reply, Reply, stamp(Key, write, NewState)};
handle_call(#edis_command{cmd = <<"RPUSHX">>, args = [Key, Value]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, list,
           fun(Item) ->
                   {edis_lists:length(Item#edis_item.value) + 1,
                    Item#edis_item{value =
                                     edis_lists:reverse(
                                       edis_lists:push(
                                         Value, edis_lists:reverse(Item#edis_item.value)))}}
           end, 0),
  {reply, Reply, stamp(Key, write, State)};
%% -- Sets -----------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"SADD">>, args = [Key | Members]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, set, hashtable,
           fun(Item) ->
                   NewValue =
                     lists:foldl(fun gb_sets:add_element/2, Item#edis_item.value, Members),
                   {gb_sets:size(NewValue) - gb_sets:size(Item#edis_item.value),
                    Item#edis_item{value = NewValue}}
           end, gb_sets:new()),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"SCARD">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SDIFF">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:to_list(Value)};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SDIFF">>, args = [Key | Keys]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
      #edis_item{value = Value} ->
        {ok, gb_sets:to_list(
           lists:foldl(
             fun(SKey, AccSet) ->
                     case get_item(State#state.backend_mod, State#state.backend_ref, set, SKey) of
                       #edis_item{value = SValue} -> gb_sets:subtract(AccSet, SValue);
                       not_found -> AccSet;
                       {error, Reason} -> throw(Reason)
                     end
             end, Value, Keys))};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SDIFFSTORE">>, args = [Destination | Keys]}, From, State) ->
    case handle_call(#edis_command{cmd = <<"SDIFF">>, args = Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
        {reply, {ok, 0}, stamp(Keys, read, stamp(Destination, write, NewState))};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
            case (State#state.backend_mod):put(
                   State#state.backend_ref,
                   Destination,
                   #edis_item{key = Destination, type = set, encoding = hashtable, value = Value}) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp(Keys, read, stamp(Destination, write, NewState))};
      ErrorReply ->
        ErrorReply
    end;
handle_call(#edis_command{cmd = <<"SINTER">>, args = Keys}, _From, State) ->
  Reply =
    try gb_sets:intersection(
          [case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
             #edis_item{value = Value} -> Value;
             not_found -> throw(empty);
             {error, Reason} -> throw(Reason)
           end || Key <- Keys]) of
      Set -> {ok, gb_sets:to_list(Set)}
    catch
      _:empty -> {ok, []};
      _:Error -> {error, Error}
    end,
  {reply, Reply, stamp(Keys, read, State)};
handle_call(#edis_command{cmd = <<"SINTERSTORE">>, args = [Destination | Keys]}, From, State) ->
    case handle_call(#edis_command{cmd = <<"SINTER">>, args = Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
        {reply, {ok, 0}, stamp(Keys, read, stamp(Destination, write, NewState))};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
          case (State#state.backend_mod):put(
                 State#state.backend_ref,
                 Destination,
                 #edis_item{key = Destination, type = set, encoding = hashtable, value = Value}) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp(Keys, read, stamp(Destination, write, NewState))};
      ErrorReply ->
        ErrorReply
    end;
handle_call(#edis_command{cmd = <<"SISMEMBER">>, args = [Key, Member]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:is_element(Member, Value)};
      not_found -> {ok, false};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SMEMBERS">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
      #edis_item{value = Value} -> {ok, gb_sets:to_list(Value)};
      not_found -> {ok, []};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SMOVE">>, args = [Source, Destination, Member]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Source, set,
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
                end, false) of
      {ok, delete} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Source),
        update(State#state.backend_mod, State#state.backend_ref, Destination, set, hashtable,
               fun(Item) ->
                       {true, Item#edis_item{value =
                                               gb_sets:add_element(Member, Item#edis_item.value)}}
               end, gb_sets:empty());
      {ok, true} ->
        update(State#state.backend_mod, State#state.backend_ref, Destination, set, hashtable,
               fun(Item) ->
                       {true, Item#edis_item{value =
                                               gb_sets:add_element(Member, Item#edis_item.value)}}
               end, gb_sets:empty());
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp([Source, Destination], write, State)};
handle_call(#edis_command{cmd = <<"SPOP">>, args = [Key]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, set,
                fun(Item) ->
                        {Member, NewValue} = gb_sets:take_smallest(Item#edis_item.value),
                        case gb_sets:size(NewValue) of
                          0 -> {{delete, Member}, Item#edis_item{value = NewValue}};
                          _ -> {Member, Item#edis_item{value = NewValue}}
                        end
                end, undefined) of
      {ok, {delete, Member}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {ok, Member};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"SRANDMEMBER">>, args = [Key]}, _From, State) ->
  _ = random:seed(erlang:now()),
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
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
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"SREM">>, args = [Key | Members]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, set,
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
                end, 0) of
      {ok, {delete, Count}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {ok, Count};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"SUNION">>, args = Keys}, _From, State) ->
  Reply =
    try gb_sets:union(
          [case get_item(State#state.backend_mod, State#state.backend_ref, set, Key) of
             #edis_item{value = Value} -> Value;
             not_found -> gb_sets:empty();
             {error, Reason} -> throw(Reason)
           end || Key <- Keys]) of
      Set -> {ok, gb_sets:to_list(Set)}
    catch
      _:empty -> {ok, []};
      _:Error -> {error, Error}
    end,
  {reply, Reply, stamp(Keys, read, State)};
handle_call(#edis_command{cmd = <<"SUNIONSTORE">>, args = [Destination | Keys]}, From, State) ->
    case handle_call(#edis_command{cmd = <<"SUNION">>, args = Keys}, From, State) of
      {reply, {ok, []}, NewState} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
        {reply, {ok, 0}, stamp(Keys, read, stamp(Destination, write, NewState))};
      {reply, {ok, Members}, NewState} ->
        Value = gb_sets:from_list(Members),
        Reply =
          case (State#state.backend_mod):put(
                 State#state.backend_ref,
                 Destination,
                 #edis_item{key = Destination, type = set, encoding = hashtable, value = Value}) of
            ok -> {ok, gb_sets:size(Value)};
            {error, Reason} -> {error, Reason}
          end,
        {reply, Reply, stamp(Keys, read, stamp(Destination, write, NewState))};
      ErrorReply ->
        ErrorReply
    end;
%% -- ZSets -----------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"ZADD">>, args = [Key, SMs]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, zset, skiplist,
           fun(Item) ->
                   NewValue =
                     lists:foldl(fun zsets:enter/2, Item#edis_item.value, SMs),
                   {zsets:size(NewValue) - zsets:size(Item#edis_item.value),
                    Item#edis_item{value = NewValue}}
           end, zsets:new()),
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"ZCARD">>, args = [Key]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:size(Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZCOUNT">>, args = [Key, Min, Max]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:count(Min, Max, Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZINCRBY">>, args = [Key, Increment, Member]}, _From, State) ->
  Reply =
    update(State#state.backend_mod, State#state.backend_ref, Key, zset, skiplist,
           fun(Item) ->
                   NewScore =
                     case zsets:find(Member, Item#edis_item.value) of
                       error -> Increment;
                       {ok, Score} -> Score + Increment
                     end,
                   {NewScore, 
                    Item#edis_item{value = zsets:enter(NewScore, Member, Item#edis_item.value)}}
           end, zsets:new()),
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZINTERSTORE">>, args = [Destination, WeightedKeys, Aggregate]}, _From, State) ->
  Reply =
    try weighted_intersection(
          Aggregate,
          [case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
              #edis_item{value = Value} -> {Value, Weight};
              not_found -> throw(empty);
              {error, Reason} -> throw(Reason)
            end || {Key, Weight} <- WeightedKeys]) of
      ZSet ->
        case zsets:size(ZSet) of
          0 ->
            _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
            {ok, 0};
          Size ->
            case (State#state.backend_mod):put(
                   State#state.backend_ref,
                   Destination,
                   #edis_item{key = Destination, type = zset, encoding = skiplist, value = ZSet}) of
              ok -> {ok, Size};
              {error, Reason} -> {error, Reason}
            end
        end
    catch
      _:empty ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
        {ok, 0};
      _:Error ->
        ?ERROR("~p~n", [Error]),
        {error, Error}
    end,
  {reply, Reply, stamp(Destination, write, stamp([Key || {Key, _} <- WeightedKeys], read, State))};
handle_call(#edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop | Options]}, _From, State) ->
  Reply =
    try
      case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
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
            StopPos -> {ok,
                        case lists:member(with_scores, Options) of
                          true ->
                            lists:flatmap(fun tuple_to_list/1, zsets:range(StartPos, StopPos, Value));
                          false ->
                            [Member || {_Score, Member} <- zsets:range(StartPos, StopPos, Value)]
                        end}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [Key, Min, Max | _Options]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:list(Min, Max, Value)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZRANK">>, args = [Key, Member]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, zsets:count(neg_infinity, {exc, Score}, Value)}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZREM">>, args = [Key | Members]}, _From, State) ->
  Reply =
    case update(State#state.backend_mod, State#state.backend_ref, Key, zset,
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
                end, 0) of
      {ok, {delete, Count}} ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Key),
        {ok, Count};
      OtherReply ->
        OtherReply
    end,
  {reply, Reply, stamp(Key, write, State)};
handle_call(#edis_command{cmd = <<"ZREMRANGEBYRANK">>, args = [Key, Start, Stop]}, From, State) ->
  case handle_call(#edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop]}, From, State) of
    {reply, {ok, SMs}, NewState} ->
      handle_call(#edis_command{cmd = <<"ZREM">>,
                                args = [Key | [Member || {_Score, Member} <- SMs]]}, From, NewState);
    OtherReply ->
      OtherReply
  end;
handle_call(#edis_command{cmd = <<"ZREMRANGEBYSCORE">>, args = [Key, Min, Max]}, From, State) ->
  case handle_call(#edis_command{cmd = <<"ZRANGEBYSCORE">>,
                                 args = [Key, Min, Max]}, From, State) of
    {reply, {ok, SMs}, NewState} ->
      handle_call(#edis_command{cmd = <<"ZREM">>,
                                args = [Key | [Member || {_Score, Member} <- SMs]]}, From, NewState);
    OtherReply ->
      OtherReply
  end;
handle_call(#edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop | Options]}, _From, State) ->
  Reply =
    try
      case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
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
            StopPos -> {ok,
                        case lists:member(with_scores, Options) of
                          true ->
                            lists:flatmap(fun tuple_to_list/1,
                                          zsets:range(StartPos, StopPos, Value, backwards));
                          false ->
                            [Member || {_Score, Member} <- zsets:range(StartPos, StopPos, Value, backwards)]
                        end}
          end;
        not_found -> {ok, []};
        {error, Reason} -> {error, Reason}
      end
    catch
      _:empty -> {ok, []}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZREVRANGEBYSCORE">>, args = [Key, Min, Max | _Options]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} -> {ok, zsets:list(Min, Max, Value, backwards)};
      not_found -> {ok, 0};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZREVRANK">>, args = [Key, Member]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, zsets:count(infinity, {exc, Score}, Value, backwards)}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZSCORE">>, args = [Key, Member]}, _From, State) ->
  Reply =
    case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
      #edis_item{value = Value} ->
        case zsets:find(Member, Value) of
          error -> {ok, undefined};
          {ok, Score} -> {ok, Score}
        end;
      not_found -> {ok, undefined};
      {error, Reason} -> {error, Reason}
    end,
  {reply, Reply, stamp(Key, read, State)};
handle_call(#edis_command{cmd = <<"ZUNIONSTORE">>, args = [Destination, WeightedKeys, Aggregate]}, _From, State) ->
  Reply =
    try weighted_union(
          Aggregate,
          [case get_item(State#state.backend_mod, State#state.backend_ref, zset, Key) of
              #edis_item{value = Value} -> {Value, Weight};
              not_found -> {zsets:new(), 0.0};
              {error, Reason} -> throw(Reason)
            end || {Key, Weight} <- WeightedKeys]) of
      ZSet ->
        case zsets:size(ZSet) of
          0 ->
            _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
            {ok, 0};
          Size ->
            case (State#state.backend_mod):put(
                   State#state.backend_ref,
                   Destination,
                   #edis_item{key = Destination, type = zset, encoding = skiplist, value = ZSet}) of
              ok -> {ok, Size};
              {error, Reason} -> {error, Reason}
            end
        end
    catch
      _:empty ->
        _ = (State#state.backend_mod):delete(State#state.backend_ref, Destination),
        {ok, 0};
      _:Error ->
        ?ERROR("~p~n", [Error]),
        {error, Error}
    end,
  {reply, Reply, stamp(Destination, write, stamp([Key || {Key, _} <- WeightedKeys], read, State))};
%% -- Server ---------------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"DBSIZE">>}, _From, State) ->
  %%TODO: We need to 
  Now = edis_util:now(),
  Reply =
    {ok, (State#state.backend_mod):fold(
       State#state.backend_ref,
       fun(#edis_item{expire = Expire}, Acc) when Expire >= Now -> Acc + 1;
          (_, Acc) -> Acc
       end, 0)},
  {reply, Reply, State};
handle_call(#edis_command{cmd = <<"FLUSHDB">>}, _From, State) ->
  ok = (State#state.backend_mod):destroy(State#state.backend_ref),
  case init(State#state.index) of
    {ok, NewState} ->
      {reply, ok, NewState};
    {stop, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call(#edis_command{cmd = <<"INFO">>}, _From, State) ->
  Version =
    case lists:keyfind(edis, 1, application:loaded_applications()) of
      false -> "0";
      {edis, _Desc, V} -> V
    end,
  {ok, Stats} = (State#state.backend_mod):status(State#state.backend_ref),
  Reply =
    {ok,
     iolist_to_binary(
       [io_lib:format("edis_version: ~s~n", [Version]),
        io_lib:format("last_save: ~p~n", [State#state.last_save]),
        io_lib:format("db_stats: ~s~n", [Stats])])},
  {reply, Reply, State}; %%TODO: add info
handle_call(#edis_command{cmd = <<"LASTSAVE">>}, _From, State) ->
  {reply, {ok, erlang:round(State#state.last_save)}, State};
handle_call(#edis_command{cmd = <<"SAVE">>}, _From, State) ->
  {reply, ok, State#state{last_save = edis_util:timestamp()}};
%% -- Transactions ---------------------------------------------------------------------------------
handle_call(#edis_command{cmd = <<"EXEC">>, args = Commands}, From, State) ->
  {RevReplies, NewState} =
    lists:foldl(
      fun(Command, {AccReplies, AccState}) ->
              case handle_call(Command#edis_command{expire = undefined}, From, AccState) of
                {noreply, NextState} -> %% Timed out
                  {[{ok, undefined}|AccReplies], NextState};
                {reply, Reply, NextState} ->
                  {[Reply|AccReplies], NextState}
              end
      end, {[], State}, Commands),
  {reply, {ok, lists:reverse(RevReplies)}, NewState};

handle_call(#edis_command{}, _From, State) ->
  {reply, {error, unsupported}, State};
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
stamp(undefined, _Action, State) -> State;
stamp([], _Action, State) -> State;
stamp([Key|Keys], Action, State) ->
  stamp(Keys, Action, stamp(Key, Action, State));
stamp(_Key, none, State) -> State;
stamp(Key, read, State) ->
  State#state{accesses =
                  dict:store(Key, edis_util:now() - State#state.start_time, State#state.accesses)};
stamp(Key, write, State) ->
  State#state{updates =
                  dict:store(Key, edis_util:now() - State#state.start_time, State#state.updates),
              accesses =
                  dict:store(Key, edis_util:now() - State#state.start_time, State#state.accesses)}.

%% @private
block_list_op(_Key, _Req, _From, undefined, State) -> State;
block_list_op(Key, Req, From, Timeout, State) ->
  CurrentList =
      case dict:find(Key, State#state.blocked_list_ops) of
        error -> [];
        {ok, L} -> L
      end,
  State#state{blocked_list_ops =
                dict:store(Key,
                           lists:reverse([{Timeout, Req, From}|
                                            lists:reverse(CurrentList)]),
                           State#state.blocked_list_ops)}.

%% @private
unblock_list_ops(Key, From, State) ->
  case dict:find(Key, State#state.blocked_list_ops) of
    error -> State;
    {ok, List} ->
      State#state{blocked_list_ops =
                      dict:store(
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
  case dict:find(Key, State#state.blocked_list_ops) of
    error -> State;
    {ok, [{Timeout, Req, From = {To, _Tag}}]} when Timeout > Now ->
      case rpc:pinfo(To) of
        undefined -> %% Caller disconnected
          State#state{blocked_list_ops = dict:erase(Key, State#state.blocked_list_ops)};
        _ ->
          case handle_call(Req, From, State) of
            {reply, {error, not_found}, NewState} -> %% still have to wait
              NewState;
            {reply, Reply, NewState} ->
              gen_server:reply(From, Reply),
              NewState#state{blocked_list_ops = dict:erase(Key, NewState#state.blocked_list_ops)}
          end
      end;
    {ok, [{Timeout, Req, From = {To, _Tag}} | Rest]} when Timeout > Now ->
      case rpc:pinfo(To) of
        undefined -> %% Caller disconnected
          check_blocked_list_ops(
            Key, State#state{blocked_list_ops =
                                 dict:store(Key, Rest, State#state.blocked_list_ops)});
        _ ->
          case handle_call(Req, From, State) of
            {reply, {error, not_found}, NewState} -> %% still have to wait
              NewState;
            {reply, Reply, NewState} ->
              gen_server:reply(From, Reply),
              check_blocked_list_ops(
                Key, NewState#state{blocked_list_ops =
                                        dict:store(Key, Rest, NewState#state.blocked_list_ops)})
          end
      end;
    {ok, [_TimedOut]} ->
      State#state{blocked_list_ops = dict:erase(Key, State#state.blocked_list_ops)};
    {ok, [_TimedOut|Rest]} ->
      check_blocked_list_ops(
            Key, State#state{blocked_list_ops =
                                 dict:store(Key, Rest, State#state.blocked_list_ops)});
    {ok, []} ->
      State#state{blocked_list_ops = dict:erase(Key, State#state.blocked_list_ops)}
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
exists_item(Mod, Ref, Key) ->
  not_found /= get_item(Mod, Ref, any, Key).

%% @private
get_item(Mod, Ref, Types, Key) when is_list(Types) ->
  case get_item(Mod, Ref, any, Key) of
    Item = #edis_item{type = T} ->
      case lists:member(T, Types) of
        true -> Item;
        false -> {error, bad_item_type}
      end;
    Other ->
      Other
  end;
get_item(Mod, Ref, Type, Key) ->
  case Mod:get(Ref, Key) of
    Item = #edis_item{type = T, expire = Expire} when Type =:= any orelse T =:= Type ->
      Now = edis_util:now(),
      case Expire of
        Expire when Expire >= Now ->
          Item;
        _ ->
          _ = Mod:delete(Ref, Key),
          not_found
      end;
    #edis_item{} ->
      {error, bad_item_type};
    not_found ->
      not_found;
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
update(Mod, Ref, Key, Type, Fun) ->
  try
    {Res, NewItem} =
      case get_item(Mod, Ref, Type, Key) of
        not_found -> throw(not_found);
        {error, Reason} -> throw(Reason);
        Item -> Fun(Item)
      end,
    case Mod:put(Ref, Key, NewItem) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:Error ->
      {error, Error}
  end.

update(Mod, Ref, Key, Type, Fun, ResultIfNotFound) ->
  try
    {Res, NewItem} =
      case get_item(Mod, Ref, Type, Key) of
        not_found -> throw(not_found);
        {error, Reason} -> throw(Reason);
        Item -> Fun(Item)
      end,
    case Mod:put(Ref, Key, NewItem) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:not_found ->
      {ok, ResultIfNotFound};
    _:Error ->
      {error, Error}
  end.

%% @private
update(Mod, Ref, Key, Type, Encoding, Fun, Default) ->
  try
    {Res, NewItem} =
      case get_item(Mod, Ref, Type, Key) of
        not_found ->
          Fun(#edis_item{key = Key, type = Type, encoding = Encoding, value = Default});
        {error, Reason} ->
          throw(Reason);
        Item ->
          Fun(Item)
      end,
    case Mod:put(Ref, Key, NewItem) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:Error ->
      ?ERROR("~p~n", [Error]),
      {error, Error}
  end.

%% @private
key_at(Mod, Ref, 0) ->
  try
    Now = edis_util:now(),
    Mod:fold(
      Ref,
      fun(#edis_item{key = Key, expire = Expire}, _Acc) when Expire >= Now ->
              throw({ok, Key});
         (_, Acc) -> Acc
      end, {ok, undefined})
  catch
    _:{ok, Key} -> {ok, Key}
  end;
key_at(Mod, Ref, Index) when Index > 0 ->
  try
    Now = edis_util:now(),
    NextIndex =
      Mod:fold(
        Ref,
        fun(#edis_item{key = Key, expire = Expire}, 0) when Expire >= Now ->
                throw({ok, Key});
           (_, 0) ->
                0;
           (_, AccIndex) ->
                AccIndex - 1
        end, Index),
    key_at(Mod, Ref, NextIndex)
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

sort(_Mod, _Ref, _Item, #edis_sort_options{limit = {_Off, 0}}) -> {ok, []};
sort(_Mod, _Ref, _Item, #edis_sort_options{limit = {Off, _Lim}}) when Off < 0 -> {ok, []};
sort(Mod, Ref, #edis_item{type = Type, value = Value}, Options) ->
  Elements =
    case Type of
      list -> edis_lists:to_list(Value);
      set -> gb_sets:to_list(Value);
      zset -> [Member || {_Score, Member} <- zsets:to_list(Value)]
    end,
  Mapped =
    lists:map(fun(Element) ->
                      {retrieve(Mod, Ref, Element, Options#edis_sort_options.by), Element}
              end, Elements),
  Sorted =
    case {Options#edis_sort_options.type, Options#edis_sort_options.direction} of
      {default, asc} -> lists:sort(fun default_sort/2, Mapped);
      {default, desc} -> lists:reverse(lists:sort(fun default_sort/2, Mapped));
      {alpha, asc} -> lists:sort(Mapped);
      {alpha, desc} -> lists:reverse(lists:sort(Mapped))
    end,
  Limited =
    case Options#edis_sort_options.limit of
      undefined -> Sorted;
      {Off, _Lim} when Off >= length(Sorted) -> [];
      {Off, Lim} when Lim < 0 -> lists:nthtail(Off, Sorted);
      {Off, Lim} -> lists:sublist(Sorted, Off+1, Lim)
    end,
  Complete =
    lists:flatmap(
      fun({_Map, Element}) ->
              lists:map(
                fun(Pattern) -> retrieve(Mod, Ref, Element, Pattern) end,
                Options#edis_sort_options.get)
      end, Limited),
  {ok, Complete}.

default_sort({_Binary, _}, {undefined, _}) -> true; %% {binary() | atom()} =< atom
default_sort({undefined, _}, {_Binary, _}) -> false; %% binary() < atom()
default_sort({Bin1, _}, {Bin2, _}) ->
  edis_util:binary_to_float(Bin1, undefined) =< edis_util:binary_to_float(Bin2, undefined).

retrieve(_Mod, _Ref, Element, self) -> Element;
retrieve(Mod, Ref, Element, {KeyPattern, FieldPattern}) ->
  Key = binary:replace(KeyPattern, <<"*">>, Element, [global]),
  Field = binary:replace(FieldPattern, <<"*">>, Element, [global]),
  case get_item(Mod, Ref, hash, Key) of
    not_found -> undefined;
    {error, _Reason} -> undefined;
    Item -> case dict:find(Field, Item#edis_item.value) of
              {ok, Value} -> Value;
              error -> undefined
            end
  end;
retrieve(Mod, Ref, Element, Pattern) ->
  Key = binary:replace(Pattern, <<"*">>, Element, [global]),
  case get_item(Mod, Ref, string, Key) of
    #edis_item{type = string, value = Value} -> Value;
    not_found -> undefined;
    {error, _Reason} -> undefined
  end.
