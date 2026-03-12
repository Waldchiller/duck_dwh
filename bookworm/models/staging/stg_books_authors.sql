with source as (
    select * from {{ source('raw', 'books') }}
),

unnested as (
    select
        book_id::varchar                            as book_id,
        unnest(authors).author_id::varchar          as author_id,
        nullif(trim(unnest(authors).role), '')      as role
    from source
)

select * from unnested
