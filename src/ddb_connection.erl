%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ddb_connection).

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API
-export([start_link/1,
         pool/0,
         get/5, get/6,
         list/1, list/2,
         list_pfx/2, list_pfx/3,
         list_buckets/0, list_buckets/1,
         resolution/1, resolution/2,
         info/1, info/2
        ]).
-export([events/2, read_events/3, read_events/4]).
-ignore_xref([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TIMEOUT, 30000).
-define(MAX_COUNT, 604800).
-record(state, {connection, metrics=gb_trees:empty(), host, port,
                max_read = ?MAX_COUNT}).

%%%===================================================================
%%% API
%%%===================================================================

pool() ->
    backend_connection.

events(Bucket, Events) ->
    events(pool(), Bucket, Events).

events(Pool, Bucket, Events) ->
    Worker = worker({events, Bucket, Events}),
    transact(Pool, Worker, ?TIMEOUT).

read_events(Bucket, Start, End) ->
    read_events(Bucket, Start, End, []).
read_events(Bucket, Start, End, Filter) ->
    read_events(pool(), Bucket, Start, End, Filter).
read_events(Pool, Bucket, Start, End, Filter) ->
    Worker = worker({read_events, Bucket, Start, End, Filter}),
    transact(Pool, Worker, ?TIMEOUT).

get(Bucket, Metric, Time, Count, Parent) ->
    get(pool(), Bucket, Metric, Time, Count, Parent).

get(Pool, Bucket, Metric, Time, Count, Parent) ->
    S  = otters:start_child(ddb_connection, Parent),
    S1 = otters:log(S, "request pooled"),
    S2 = otters:tag(S1, <<"bucket">>, Bucket),
    S3 = otters:tag(S2, <<"metric">>, Metric),
    S4 = otters:tag(S3, <<"time">>, Time),
    S5 = otters:tag(S4, <<"count">>, Count),
    Worker = worker({get, Bucket, Metric, Time, Count, S5}),
    transact(Pool, Worker, ?TIMEOUT).

-spec resolution(binary()) ->
                        {ok, pos_integer()} |
                        {error, term()}.
resolution(Bucket) ->
    resolution(pool(), Bucket).

resolution(Pool, Bucket) ->
    transact(Pool,
             fun(Worker) ->
                     gen_server:call(Worker, {resolution, Bucket}, ?TIMEOUT)
             end, ?TIMEOUT).

-spec info(binary()) ->
                  {ok, dproto_tcp:bucket_info()} |
                  {error, term()}.

info(Bucket) ->
    info(pool(), Bucket).

info(Pool, Bucket) ->
    transact(Pool,
             fun(Worker) ->
                     gen_server:call(Worker, {info, Bucket}, ?TIMEOUT)
             end, ?TIMEOUT).

list(Bucket) ->
    list(pool(), Bucket).

list(Pool, Bucket) ->
    transact(Pool,
             fun(Worker) ->
                     gen_server:call(Worker, {list, Bucket}, ?TIMEOUT)
             end, ?TIMEOUT).

list_pfx(Bucket, Prefix) ->
    list_pfx(pool(), Bucket, Prefix).

list_pfx(Pool, Bucket, Prefix) ->
    transact(Pool,
             fun(Worker) ->
                     gen_server:call(Worker, {list, Bucket, Prefix}, ?TIMEOUT)
             end, ?TIMEOUT).

list_buckets() ->
    list_buckets(pool()).

list_buckets(Pool) ->
    transact(Pool,
             fun(Worker) ->
                     gen_server:call(Worker, list, ?TIMEOUT)
             end, ?TIMEOUT).

worker(Call) ->
    fun(Worker) ->
            gen_server:call(Worker, Call, ?TIMEOUT)
    end.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port]) ->
    {ok, MaxRead} = application:get_env(ddb_connection, max_read),
    {ok, #state{max_read=MaxRead, host = Host, port = Port}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({events, Bucket, Events}, _From,
            State = #state{connection = C}) ->
    case ddb_tcp:events(Bucket, Events, C) of
        {ok, C1} ->
            {reply, ok, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({read_events, Bucket, Start, End, Filter}, _From,
            State = #state{connection = C}) ->
    case ddb_tcp:read_events(Bucket, Start, End, Filter, C) of
        {ok, D, C1} ->
            {reply, {ok, D}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({get, _, _, _, Count, S}, _From, State = #state{max_read = MaxRead})
  when Count > MaxRead ->
    S1 = otters:log(S, <<"unpooled">>),
    S2 = otters:tag(S1, <<"result">>, <<"error">>),
    S3 = otters:tag(S2, <<"error">>, {error, too_big}),
    otters:finish(S3),
    {reply, {error, too_big}, State};

handle_call({get, Bucket, Metric, Time, Count, S}, _From,
            State = #state{connection = C}) ->
    TIDs = otters:ids(S),
    S1 = otters:log(S, <<"unpooled">>),
    case ddb_tcp:get(Bucket, Metric, Time, Count, [], TIDs, C) of
        {ok, D, C1} ->
            S2 = otters:tag(S1, <<"result">>, <<"success">>),
            otters:finish(S2),
            {reply, {ok, D}, State#state{connection = C1}};
        {error, E, C1} ->
            S2 = otters:tag(S1, <<"result">>, <<"error">>),
            S3 = otters:tag(S2, <<"error">>, E),
            otters:finish(S3),
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({info, Bucket}, _From,
            State = #state{connection = C}) ->
    case ddb_tcp:bucket_info(Bucket, C) of
        {ok, Info, C1} ->
            {reply, {ok, Info}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({resolution, Bucket}, _From,
            State = #state{connection = C}) ->
    case ddb_tcp:bucket_info(Bucket, C) of
        {ok, #{resolution := Resolution}, C1} ->
            {reply, {ok, Resolution}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({list, Bucket}, _From, State) ->
    case gb_trees:lookup(Bucket, State#state.metrics) of
        none ->
            {Ms, State1} = do_list(Bucket, State),
            {reply, {ok, Ms}, State1};
        {value, {LastRead, Ms}} ->
            Now = erlang:system_time(seconds),
            case Now - LastRead of
                T when T > 60  ->
                    {Ms, State1} = do_list(Bucket, State),
                    {reply, {ok, Ms}, State1};
                _ ->
                    {reply, {ok, Ms}, State}
            end
    end;

handle_call({list, Bucket, <<>>}, _From, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, C),
    {reply, {ok, Ms}, State#state{connection = C1}};

handle_call({list, Bucket, Prefix}, _From, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, Prefix, C),
    {reply, {ok, Ms}, State#state{connection = C1}};

handle_call(list, _From, State = #state{connection = C}) ->
    case ddb_tcp:list(C) of
        {ok, Bs, C1} ->
            {reply, {ok, Bs}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State = #state{host = Host, port = Port}) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {noreply, State#state{connection = C}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{connection = C}) ->
    _ = ddb_tcp:close(C),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


do_list(Bucket, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, C),
    Tree1 = gb_trees:enter(Bucket, {erlang:system_time(seconds), Ms},
                           State#state.metrics),
    {Ms, State#state{metrics = Tree1, connection = C1}}.


%% This is needed for riak_core applications, they depends on a
%% poolboy old as shit that does not support transaction/3.
%%
%% The following code is exactly how transaction/3 works on newer
%% poolboy versions
transact(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.
