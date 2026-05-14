ALTER TABLE store_sales CREATE INDEX idx_ss_sold_date_sk USING zonemap (ss_sold_date_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_item_sk USING zonemap (ss_item_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_customer_sk USING zonemap (ss_customer_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_store_sk USING zonemap (ss_store_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_promo_sk USING zonemap (ss_promo_sk);
