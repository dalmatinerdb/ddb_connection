%%%-------------------------------------------------------------------
%% @doc ddb_connection top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(ddb_connection_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, endpoint/0]).

%% Supervisor callbacks
-export([init/1]).

-ignore_xref([endpoint/0]).


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {Host, Port} = endpoint(),
    {ok, PoolSize} = application:get_env(ddb_connection, pool_size),
    {ok, PoolMax} = application:get_env(ddb_connection, pool_max),
    PoolName = ddb_connection:pool(),
    SizeArgs = [
                {size, PoolSize},
                {max_overflow, PoolMax}
               ],
    PoolArgs = [{name, {local, PoolName}},
                {worker_module, ddb_connection}] ++ SizeArgs,
    WorkerArgs = [Host, Port],
    {ok, {{one_for_one, 5, 10},
          [poolboy:child_spec(PoolName, PoolArgs, WorkerArgs)]}}.

endpoint() ->
    case application:get_env(ddb_connection, backend) of
        {ok, {Host, Port}} ->
            {Host, Port};
        _ ->
            {ok, Host} = application:get_env(ddb_connection,
                                             backend_host),
            {ok, Port} = application:get_env(ddb_connection,
                                             backend_port),
            {Host, Port}
    end.
