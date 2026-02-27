# kind.co.kr 유상증자 데이터 크롤링
    - 정해진 기간 동안에서의 유상증자 데이터를 크롤링을 해옵니다.
#### Stack
    python 3.11.5
    DB PostGresql 18

#### DB DDL
    CREATE TABLE public.kind_paid_in_capital (
        id serial4 NOT NULL,
        reference_date date NOT NULL,
        company_name varchar(200) NOT NULL,
        increase_type varchar(100) NULL,
        stock_type varchar(100) NULL,
        issued_shares int8 NULL,
        allotment_ratio numeric(20, 6) NULL,
        employee_subscribe_date date NULL,
        existing_holder_date date NULL,
        payment_date date NULL,
        original_link text NULL,
        crawled_at timestamptz DEFAULT now() NULL,
        CONSTRAINT kind_paid_in_capital_pkey PRIMARY KEY (id),
        CONSTRAINT uq_kind_paid_in_capital UNIQUE (reference_date, company_name, payment_date)
    );
    CREATE INDEX idx_kind_pic_company_name ON public.kind_paid_in_capital USING btree (company_name);
    CREATE INDEX idx_kind_pic_reference_date ON public.kind_paid_in_capital USING btree (reference_date);
