create table movie (
    movie_id serial primary key,
    title varchar(300) not null,
    release_date date not null,
    imdb_url varchar(300) unique not null,
    type varchar(100) not null,
    country varchar(100),
    created_at timestamp with time zone default current_timestamp,
    updated_at timestamp with time zone default current_timestamp,
    description varchar(500)
);

create table crawler_log (
    log_id serial primary key,
    parsed_at timestamp with time zone default current_timestamp,
    movies_added int default 0,
    movies_updated int default 0,
    error_message varchar(1000)
);

create table person (
    person_id serial primary key,
    full_name varchar(200) not null,
    imdb_id varchar(200) not null
);

create table movie_cast (
    movie_id int references movie(movie_id) on delete cascade,
    person_id int references person(person_id) on delete cascade,
    role varchar(100) not null,
    primary key (movie_id, person_id)
);

create table review (
    review_id serial primary key,
    movie_id int references movie(movie_id) on delete cascade,
    rating int not null check (rating >= 1 and rating <= 10),
    review_text varchar(1000),
    created_at timestamp with time zone default current_timestamp,
    updated_at timestamp with time zone default current_timestamp
);

create table crawler_task_status (
    task_id serial primary key,
    imdb_url varchar(300) unique not null,
    status varchar(50) default 'pending' check (status in ('pending', 'in_progress', 'done', 'failed')),
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    error_message varchar(1000),
    last_updated timestamp with time zone default current_timestamp,
    assigned_instance varchar(100)
);
