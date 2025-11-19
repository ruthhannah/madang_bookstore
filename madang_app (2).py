import streamlit as st
import duckdb
import pandas as pd
import datetime
import time

# ==========================================
# 1. 사용자 기본 정보 (본인 이름 필수 수정!)
# ==========================================
my_name = "최혜원"   # <--- 본인 이름으로 수정하세요!
my_address = "대한민국 인천시"
my_phone = "010-1234-5678"

# ==========================================
# 2. DuckDB 연결 및 "완전 초기화" (Reset)
# ==========================================
# 주의: 이 코드는 실행할 때마다 데이터를 초기화해서 꼬임을 방지합니다.
con = duckdb.connect(database='madang.db', read_only=False)

# 기존 테이블이 있다면 삭제 (박지성 복구를 위해 싹 지웁니다)
con.execute("DROP TABLE IF EXISTS Orders")
con.execute("DROP TABLE IF EXISTS Customer")
con.execute("DROP TABLE IF EXISTS Book")

# 테이블 새로 생성
con.execute("""
CREATE TABLE Book (bookid INTEGER, bookname VARCHAR, publisher VARCHAR, price INTEGER);
CREATE TABLE Customer (custid INTEGER, name VARCHAR, address VARCHAR, phone VARCHAR);
CREATE TABLE Orders (orderid INTEGER, custid INTEGER, bookid INTEGER, saleprice INTEGER, orderdate VARCHAR);
""")

# ==========================================
# 3. 데이터 입력 (박지성 + 나)
# ==========================================

# (1) 책 데이터 (기존 그대로)
books = [
    (1, '축구의 역사', '굿스포츠', 7000), (2, '축구아는 여자', '나무수', 13000),
    (3, '축구의 이해', '대한미디어', 22000), (4, '골프 바이블', '대한미디어', 35000),
    (5, '피겨 교본', '굿스포츠', 8000), (6, '역도 단계별기술', '굿스포츠', 6000),
    (7, '야구의 추억', '이상미디어', 20000), (8, '야구를 부탁해', '이상미디어', 13000),
    (9, '올림픽 이야기', '삼성당', 7500), (10, 'Olympic Champions', 'Pearson', 13000)
]
con.executemany("INSERT INTO Book VALUES (?, ?, ?, ?)", books)

# (2) 고객 데이터 (★ 박지성 살려내고, 나를 6번에 추가)
customers = [
    (1, '박지성', '영국 맨체스타', '000-5000-0001'),  # <--- 박지성 부활!
    (2, '김연아', '대한민국 서울', '000-6000-0001'),
    (3, '장미란', '대한민국 강원도', '000-7000-0001'),
    (4, '추신수', '미국 클리블랜드', '000-8000-0001'),
    (5, '박세리', '대한민국 대전', None),
    (6, my_name, my_address, my_phone)              # <--- 6번에 본인 추가
]
con.executemany("INSERT INTO Customer VALUES (?, ?, ?, ?)", customers)

# (3) 주문 데이터
orders = [
    (1, 1, 1, 6000, '2014-07-01'), (2, 1, 3, 21000, '2014-07-03'),
    (3, 2, 5, 8000, '2014-07-03'), (4, 3, 6, 6000, '2014-07-04'),
    (5, 4, 7, 20000, '2014-07-05'), (6, 1, 2, 12000, '2014-07-07'),
    (7, 4, 8, 13000, '2014-07-07'), (8, 3, 10, 12000, '2014-07-08'),
    (9, 2, 10, 7000, '2014-07-09'), (10, 3, 8, 13000, '2014-07-10')
]
con.executemany("INSERT INTO Orders VALUES (?, ?, ?, ?, ?)", orders)

# (4) 나의 구매 내역 추가 (6번 고객이 10번 책 구매)
dt = datetime.date.today().strftime("%Y-%m-%d")
con.execute(f"INSERT INTO Orders VALUES (11, 6, 10, 13000, '{dt}')")


# ==========================================
# 4. 화면 구성 (UI)
# ==========================================
st.set_page_config(page_title="마당 북스토어", page_icon="📚", layout="wide")

st.title("📚 마당 북스토어")
st.caption(f"Welcome, {my_name} 님 👋")

# 사이드바 - 심플하게 정리
with st.sidebar:
    st.header("📋 메뉴")
    menu = st.radio("", ["🏠 홈", "🔍 고객 조회", "🛒 주문하기", "➕ 고객 등록"], label_visibility="collapsed")
    
    st.divider()
    st.caption("📊 빠른 통계")
    total_customers = con.execute("SELECT COUNT(*) FROM Customer").fetchone()[0]
    total_orders = con.execute("SELECT COUNT(*) FROM Orders").fetchone()[0]
    total_sales = con.execute("SELECT SUM(saleprice) FROM Orders").fetchone()[0]
    
    st.metric("총 고객", f"{total_customers}명")
    st.metric("총 주문", f"{total_orders}건")
    st.metric("총 매출", f"{total_sales:,}원")

# ==========================================
# 홈 화면
# ==========================================
if menu == "🏠 홈":
    st.header("📊 대시보드")
    
    # 상단 메트릭 카드
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📚 전체 도서", con.execute("SELECT COUNT(*) FROM Book").fetchone()[0])
    with col2:
        st.metric("👥 전체 고객", total_customers)
    with col3:
        st.metric("📦 전체 주문", total_orders)
    with col4:
        avg_price = con.execute("SELECT AVG(saleprice) FROM Orders").fetchone()[0]
        st.metric("💰 평균 주문액", f"{int(avg_price):,}원")
    
    st.divider()
    
    # 차트 섹션
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("📈 출판사별 도서 수")
        pub_data = con.execute("""
            SELECT publisher, COUNT(*) as count 
            FROM Book 
            GROUP BY publisher 
            ORDER BY count DESC
        """).df()
        st.bar_chart(pub_data.set_index('publisher'))
    
    with col_chart2:
        st.subheader("💸 고객별 구매 금액")
        cust_sales = con.execute("""
            SELECT c.name, SUM(o.saleprice) as total
            FROM Customer c
            LEFT JOIN Orders o ON c.custid = o.custid
            GROUP BY c.name
            ORDER BY total DESC
        """).df()
        st.bar_chart(cust_sales.set_index('name'))
    
    st.divider()
    
    # 최근 주문 내역
    st.subheader("🕒 최근 주문 내역")
    recent_orders = con.execute("""
        SELECT c.name as 고객명, b.bookname as 도서명, 
               o.saleprice as 가격, o.orderdate as 주문일
        FROM Orders o
        JOIN Customer c ON o.custid = c.custid
        JOIN Book b ON o.bookid = b.bookid
        ORDER BY o.orderid DESC
        LIMIT 10
    """).df()
    st.dataframe(recent_orders, use_container_width=True, hide_index=True)

# ==========================================
# 고객 조회
# ==========================================
elif menu == "🔍 고객 조회":
    st.header("🔍 고객 조회")
    
    search = st.text_input("🔎 고객 이름 검색", placeholder="예: 박지성, 최혜원")
    
    if search:
        cust = con.execute(f"SELECT * FROM Customer WHERE name='{search}'").df()
        if not cust.empty:
            # 고객 정보 카드
            st.success(f"✅ 고객을 찾았습니다!")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"**고객 ID**  \n{cust['custid'][0]}")
            with col2:
                st.info(f"**주소**  \n{cust['address'][0]}")
            with col3:
                phone = cust['phone'][0] if cust['phone'][0] else "미등록"
                st.info(f"**전화번호**  \n{phone}")
            
            st.divider()
            
            # 구매 내역
            st.subheader(f"📚 {search}님의 구매 내역")
            sql = f"""
            SELECT o.orderid as 주문번호, b.bookname as 도서명, 
                   o.saleprice as 가격, o.orderdate as 주문일 
            FROM Orders o, Book b, Customer c
            WHERE o.bookid=b.bookid AND o.custid=c.custid AND c.name='{search}'
            ORDER BY o.orderdate DESC
            """
            orders_df = con.execute(sql).df()
            
            if not orders_df.empty:
                st.dataframe(orders_df, use_container_width=True, hide_index=True)
                total = orders_df['가격'].sum()
                st.success(f"💰 총 구매 금액: **{total:,}원** ({len(orders_df)}건)")
            else:
                st.warning("구매 내역이 없습니다.")
        else:
            st.error("❌ 찾는 고객이 없습니다.")
    else:
        st.info("💡 위 검색창에 고객 이름을 입력하세요.")
        
        # 전체 고객 목록
        st.subheader("📋 전체 고객 목록")
        all_customers = con.execute("""
            SELECT custid as ID, name as 이름, address as 주소, phone as 전화번호
            FROM Customer
            ORDER BY custid
        """).df()
        st.dataframe(all_customers, use_container_width=True, hide_index=True)

# ==========================================
# 주문하기
# ==========================================
elif menu == "🛒 주문하기":
    st.header("🛒 새 주문 등록")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 고객 선택
        c_list = con.execute("SELECT name FROM Customer ORDER BY custid").df()['name'].tolist()
        who = st.selectbox("👤 구매자 선택", c_list, index=c_list.index(my_name) if my_name in c_list else 0)
    
    with col2:
        # 책 선택
        b_df = con.execute("SELECT bookid, bookname, price FROM Book ORDER BY bookid").df()
        b_opts = [f"{r['bookname']} - {r['price']:,}원" for i,r in b_df.iterrows()]
        book_str = st.selectbox("📚 도서 선택", b_opts)
    
    st.divider()
    
    # 주문 확인
    selected_book = book_str.split(" - ")[0]
    selected_price = int(book_str.split(" - ")[1].replace("원", "").replace(",", ""))
    
    st.info(f"**주문 내역**  \n👤 구매자: {who}  \n📚 도서: {selected_book}  \n💰 금액: {selected_price:,}원")
    
    if st.button("✅ 주문 완료", type="primary", use_container_width=True):
        try:
            c_id = con.execute(f"SELECT custid FROM Customer WHERE name='{who}'").fetchone()[0]
            b_id = con.execute(f"SELECT bookid FROM Book WHERE bookname='{selected_book}'").fetchone()[0]
            o_id = con.execute("SELECT MAX(orderid) FROM Orders").fetchone()[0] + 1
            now = datetime.date.today().strftime("%Y-%m-%d")
            
            con.execute(f"INSERT INTO Orders VALUES ({o_id}, {c_id}, {b_id}, {selected_price}, '{now}')")
            st.success("🎉 주문이 완료되었습니다!")
            time.sleep(1.5)
            st.rerun()
        except Exception as e:
            st.error(f"❌ 주문 중 오류 발생: {e}")

# ==========================================
# 고객 등록
# ==========================================
elif menu == "➕ 고객 등록":
    st.header("➕ 신규 고객 등록")
    
    with st.form("new_customer_form", clear_on_submit=True):
        nm = st.text_input("👤 이름", placeholder="홍길동")
        ad = st.text_input("🏠 주소", placeholder="서울특별시 강남구")
        ph = st.text_input("📞 전화번호", placeholder="010-1234-5678")
        
        submitted = st.form_submit_button("✅ 등록하기", type="primary", use_container_width=True)
        
        if submitted:
            if nm and ad:
                try:
                    mx = con.execute("SELECT MAX(custid) FROM Customer").fetchone()[0] + 1
                    phone_val = f"'{ph}'" if ph else "NULL"
                    con.execute(f"INSERT INTO Customer VALUES ({mx}, '{nm}', '{ad}', {phone_val})")
                    st.success(f"🎉 {nm}님이 성공적으로 등록되었습니다!")
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 등록 중 오류 발생: {e}")
            else:
                st.warning("⚠️ 이름과 주소는 필수 입력 항목입니다.")
