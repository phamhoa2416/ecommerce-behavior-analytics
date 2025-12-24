CREATE TABLE ecommerce_events
(
    id            SERIAL PRIMARY KEY,
    event_time    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type    VARCHAR(50) NOT NULL,
    product_id    BIGINT      NOT NULL,
    category_id   BIGINT,
    category_code VARCHAR(255),
    brand         VARCHAR(255),
    price         DECIMAL(10, 2),
    user_id       BIGINT      NOT NULL,
    user_session  VARCHAR(255)
);

CREATE INDEX idx_event_time ON ecommerce_events (event_time);
CREATE INDEX idx_user_id ON ecommerce_events (user_id);
CREATE INDEX idx_product_id ON ecommerce_events (product_id);