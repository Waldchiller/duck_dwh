with source as (
    select * from {{ source('raw', 'books') }}
),

unnested as (
    select
        book_id::varchar                        as fk_book_id,
        unnest(popular_shelves).name::varchar   as shelf_name,
        unnest(popular_shelves).count::int      as shelf_count
    from source
)

select * from unnested
