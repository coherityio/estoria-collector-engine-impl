

create table if not exists collection_run
(
    run_id varchar(128) primary key,
    provider_id varchar(128) not null,
    provider_context varchar(128) not null,
    status varchar(32) not null,
    run_start_time timestamp with time zone not null,
    run_end_time timestamp with time zone,
    total_collector_count bigint not null default 0,
    successful_collector_count bigint not null default 0,
    failed_collector_count bigint not null default 0,
    total_entity_count bigint not null default 0,
    failure_message clob,
    failure_exception_class varchar(512)
);

create index if not exists idx_collection_run_provider_id
    on collection_run(provider_id);

create index if not exists idx_collection_run_status
    on collection_run(status);

create index if not exists idx_collection_run_start_time
    on collection_run(run_start_time);



create table if not exists collection_result
(
    result_id varchar(128) primary key,
    run_id varchar(128) not null,
    collector_id varchar(128) not null,
    collector_context varchar(128) not null,
    entity_type varchar(256) not null,
    status varchar(32) not null,
    entity_count bigint not null default 0,
    collection_start_time timestamp with time zone not null,
    collection_end_time timestamp with time zone,
    failure_message clob,
    failure_exception_class varchar(512),

    constraint fk_collection_result_run
        foreign key (run_id)
        references collection_run(run_id)
        on delete cascade
);

create unique index if not exists uq_collection_result_run_collector
    on collection_result(run_id, collector_id);

create index if not exists idx_collection_result_run_id
    on collection_result(run_id);

create index if not exists idx_collection_result_entity_type
    on collection_result(entity_type);

create index if not exists idx_collection_result_status
    on collection_result(status);

create index if not exists idx_collection_result_run_entity_type
    on collection_result(run_id, entity_type);



create table if not exists collected_entity
(
    result_id varchar(128) not null,
    entity_ordinal bigint not null,
    entity_id varchar(512),
    entity_type varchar(256) not null,
    payload_json clob not null,

    constraint pk_collected_entity
        primary key (result_id, entity_ordinal),

    constraint fk_collected_entity_result
        foreign key (result_id)
        references collection_result(result_id)
        on delete cascade
);

create index if not exists idx_collected_entity_result_ordinal
    on collected_entity(result_id, entity_ordinal);

create index if not exists idx_collected_entity_result_entity_id
    on collected_entity(result_id, entity_id);

create index if not exists idx_collected_entity_result_entity_type_ordinal
    on collected_entity(result_id, entity_type, entity_ordinal);