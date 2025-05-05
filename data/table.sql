create table if not exists public.commit
(
    hash      text    not null,
    message   text    not null,
    releaseid integer not null
        constraint commit_fk_releaseid
            references public.release
);

create table public.release
(
    id      serial
        primary key,
    content text    not null,
    repoid  integer not null
        constraint release_fk_repoid
            references public.repo
);

create table public.repo
(
    id     serial
        primary key,
    "user" text not null,
    name   text not null,
    star   integer
);