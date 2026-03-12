select 
    --b.book_id,
    b.title,
    --shelf_name,
    sum(s.shelf_count) AS shelf_count
from
    staging.stg_books b
join
    staging.stg_books_shelves s
     on b.book_id = s.fk_book_id
group by
    --b.book_id,
    b.title,
order by
    shelf_count desc
limit 10