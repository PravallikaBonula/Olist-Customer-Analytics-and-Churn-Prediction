--Creating tables
--customers
drop table if exists customers;
create table customers(
customer_id Text primary key,
customer_unique_id Text,
customer_zip_code_prefix integer,
customer_city text,
customer_state text
);

--orders
drop table if exists orders;
create table orders(
order_id text primary key,
customer_id text,
order_status text,
order_purchase_timestamp timestamp,
order_approved_at timestamp,
order_delivered_carrier_date timestamp,
order_delivered_customer_date timestamp,
order_estimated_delivery_date timestamp
);

--order_items
drop table if exists order_items;
create table order_items(
order_id text,
order_item_id integer,
product_id text,
seller_id text,
shipping_limit_date timestamp,
price numeric,
freight_value numeric
);

--order_payments
drop table if exists order_payments;
create table order_payments(
order_id text,
payment_sequential integer,
payment_type text,
payment_installments integer,
payment_value numeric
);

--reviews
drop table if exists reviews;
create table reviews(
review_id text,
order_id text,
review_score integer,
review_comment_title text,
review_comment_message text,
review_creation_date timestamp,
review_answer_timestamp timestamp
);

--sanity check
select * from customers;
select * from order_items;
select * from order_payments;
select * from orders;
select * from reviews;
--counts
select count(*) from customers
select count(*) from order_items
select count(*) from order_payments
select count(*) from orders
select count(*) from reviews
--distinct values
select count(distinct(customer_id)) from customers
select count(distinct(order_id)) from orders
--sample rows
select * from customers
limit 5

--creating a customer profile table customer_profile
drop view if exists customer_profile;

create view customer_profile as
with

order_totals as (
select order_items.order_id,
sum(price) as order_total_price,
sum(freight_value) as order_total_freight,
max(customer_id) as customer_id,
max(order_purchase_timestamp) as order_purchase_timestamp,
max(order_delivered_customer_date) as order_delivered_customer_date,
max(order_estimated_delivery_date) as order_estimated_delivery_date,
max(order_status) as order_status
from order_items
join orders
on orders.order_id = order_items.order_id
group by order_items.order_id
),

payments_agg as(
select order_id,
sum(payment_value) as payment_total_value
from order_payments
group by order_id
),

reviews_agg as(
select order_id,
avg(review_score) as order_review_score
from reviews
group by order_id
),

cust_orders as(
select c.customer_unique_id,
o.order_id,
ot.order_purchase_timestamp,
ot.order_total_price,
coalesce(p.payment_total_value , ot.order_total_price) as payment_value,
ra.order_review_score,
ot.order_delivered_customer_date,
ot.order_estimated_delivery_date,

case
when ot.order_delivered_customer_date is not null
and ot.order_estimated_delivery_date is not null
then (ot.order_delivered_customer_date::date - ot.order_estimated_delivery_date::date)
else null
end as delivery_delay_days,
ot.order_status
from orders o
join customers c
on o.customer_id = c.customer_id
join order_totals ot
on o.order_id = ot.order_id
left join payments_agg p
on ot.order_id = p.order_id
left join reviews_agg ra
on ot.order_id = ra.order_id
)
select 
co.customer_unique_id,
count(distinct co.order_id) as total_orders,
sum(co.order_total_price) as total_spend,
sum(co.payment_value) as total_payment_value,
max(co.order_purchase_timestamp) as last_order_date,
min(co.order_purchase_timestamp) as first_order_date,
avg(co.order_review_score) as avg_review_score,
avg(co.delivery_delay_days) as avg_delivery_delay_days,
count(case when co.order_status = 'delivered' then 1 end) as delivered_order_count,
count(case when co.order_status is null or co.order_status <> 'delivered' then 1 end ) as non_delivered_orders_count
from cust_orders co
group by co.customer_unique_id;

select count(*) from customer_profile;
select * from customer_profile 
order by total_spend desc
limit 10;
