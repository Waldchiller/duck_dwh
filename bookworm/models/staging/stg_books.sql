with source as (
    select * from {{ source('raw', 'books') }}
)

select
    book_id::varchar                                as book_id,
    work_id::varchar                                as work_id,
    title::varchar                                  as title,
    title_without_series::varchar                   as title_without_series,
    nullif(trim(isbn), '')::varchar                 as isbn,
    nullif(trim(isbn13), '')::varchar               as isbn13,
    nullif(trim(asin), '')::varchar                 as asin,
    nullif(trim(kindle_asin), '')::varchar          as kindle_asin,

    case
        when nullif(trim(publication_year), '') is null then null
        when nullif(trim(publication_month), '') is not null
             and nullif(trim(publication_day), '') is not null
            then make_date(
                publication_year::int,
                publication_month::int,
                publication_day::int
            )
        when nullif(trim(publication_month), '') is not null
            then make_date(publication_year::int, publication_month::int, 1)
        else make_date(publication_year::int, 1, 1)
    end                                             as publication_date,

    nullif(trim(description), '')::varchar          as description,
    nullif(trim(format), '')::varchar               as format,
    nullif(trim(num_pages), '')::int                as num_pages,
    nullif(trim(edition_information), '')::varchar  as edition_information,
    nullif(trim(publisher), '')::varchar            as publisher,
    average_rating::double                          as average_rating,
    ratings_count::int                              as ratings_count,
    text_reviews_count::int                         as text_reviews_count,
    (is_ebook = 'true')::boolean                    as is_ebook,
    nullif(trim(language_code), '')::varchar        as language_code,
    nullif(trim(country_code), '')::varchar         as country_code,
    image_url::varchar                              as image_url,
    link::varchar                                   as link

from source
