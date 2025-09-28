import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from datetime import datetime
from google.cloud import bigquery
import requests
import uuid

# Set page config
st.set_page_config(
    page_title="نموذج بيانات الدفع",
    page_icon="💰",
    layout="wide"
)


# Function to load dealer data from BigQuery
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_dealers():
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found for BigQuery access")
                return [], {}

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Query to get dealers
        query = """
        SELECT DISTINCT dealer_code, dealer_name
        FROM `pricing-338819.ajans_dealers.dealers`
        WHERE dealer_code IS NOT NULL AND dealer_name IS NOT NULL
        ORDER BY dealer_name
        """

        # Execute query
        dealers_data = client.query(query).to_dataframe()

        # Convert to list of dicts for compatibility
        dealers_list = dealers_data.to_dict('records')

        # Create dealer dictionary
        dealers_dict = dict(zip(dealers_data['dealer_code'], dealers_data['dealer_name']))

        return dealers_list, dealers_dict

    except Exception as e:
        st.error(f"خطأ في تحميل بيانات التجار: {str(e)}")
        return [], {}


# Function to load discount eligible cars from BigQuery
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_discount_eligible_cars():
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found for BigQuery access")
                return []

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Query to get discount eligible cars
        query = """
        SELECT 
            sf_vehicle_name,
            showroom_displayed_count,
            days_in_consignment,
            queue_count,
            discount_eligibility_flag,
            car_status
        FROM `pricing-338819.wholesale_test.showroom_discount_eligibility`
        WHERE discount_eligibility_flag = TRUE
        ORDER BY sf_vehicle_name
        """

        # Execute query
        cars_data = client.query(query).to_dataframe()
        return cars_data.to_dict('records')

    except Exception as e:
        st.error(f"خطأ في تحميل بيانات السيارات المؤهلة للخصم: {str(e)}")
        return []


# Function to load discount data from BigQuery
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_discount_data():
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found for BigQuery access")
                return []

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Query to get discount data
        query = """
        SELECT 
            c_code,
            flash_price,
            consignment_price,
            speed_discount_price
        FROM `pricing-338819.wholesale_test.showroom_discount`
        ORDER BY c_code
        """

        # Execute query
        discount_data = client.query(query).to_dataframe()
        return discount_data.to_dict('records')

    except Exception as e:
        st.error(f"خطأ في تحميل بيانات الخصم: {str(e)}")
        return []


# Function to load car names from BigQuery
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_car_names():
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found for BigQuery access")
                return []

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Query to get car names with details from live cars
        query = """
        with publishing AS (
        SELECT sf_vehicle_name,
               publishing_state,
               MAX(published_at) over (partition by sf_vehicle_name) AS max_publish_date
        FROM ajans_dealers.ajans_wholesale_to_retail_publishing_logs
        WHERE sf_vehicle_name NOT in ("C-32211","C-32203") 
        QUALIFY published_at = max_publish_date
        ),

        live_cars AS (
        SELECT sf_vehicle_name,
               type AS live_status
        FROM reporting.ajans_vehicle_history 
        WHERE date_key = current_date() ),

        car_info AS (
        with max_date AS (
        SELECT sf_vehicle_name,
               make,
               model,
               year,
               row_number()over(PARTITION BY sf_vehicle_name ORDER BY event_date DESC) AS row_number
        FROM ajans_dealers.vehicle_activity )

        SELECT *
        FROM max_date WHERE row_number = 1 )

        SELECT DISTINCT publishing.sf_vehicle_name,
               COALESCE(car_info.make, 'Unknown') as make,
               COALESCE(car_info.model, 'Unknown') as model,
               COALESCE(car_info.year, 0) as year
        FROM publishing
        LEFT JOIN live_cars ON publishing.sf_vehicle_name = live_cars.sf_vehicle_name
        LEFT JOIN reporting.vehicle_acquisition_to_selling a ON publishing.sf_vehicle_name = a.car_name
        LEFT JOIN car_info ON publishing.sf_vehicle_name = car_info.sf_vehicle_name
        WHERE allocation_category = "Wholesale" AND current_status in ("Published" , "Being Sold")
        ORDER BY publishing.sf_vehicle_name
        """

        try:
            # Execute query
            cars_data = client.query(query).to_dataframe()
            return cars_data.to_dict('records')
        except Exception as e:
            # If query fails, provide some sample car names
            st.warning("Could not load car names from BigQuery. Using sample data.")
            return [
                {"sf_vehicle_name": "C-12345", "make": "Toyota", "model": "Camry", "year": 2020},
                {"sf_vehicle_name": "C-12346", "make": "Honda", "model": "Civic", "year": 2019},
                {"sf_vehicle_name": "C-12347", "make": "Nissan", "model": "Altima", "year": 2021}
            ]

    except Exception as e:
        st.error(f"خطأ في تحميل بيانات السيارات: {str(e)}")
        return []


# Function to submit discount data to webhook
def submit_discount_data(discount_data):
    try:
        webhook_url = "https://anasalaa.app.n8n.cloud/webhook-test/9296d4cc-ca48-4bd6-9635-3ef4029b0fce"

        # Set proper headers for the webhook request
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Streamlit-Showroom-Discount/1.0'
        }

        # Debug: Log the payload being sent
        st.write("🔍 Debug Info:")
        st.write(f"Webhook URL: {webhook_url}")
        st.write("Payload being sent:")
        st.json(discount_data)

        response = requests.post(webhook_url, json=discount_data, headers=headers, timeout=10)

        # Debug: Log response details
        st.write(f"Response Status Code: {response.status_code}")
        st.write(f"Response Headers: {dict(response.headers)}")

        if response.status_code == 404:
            return False, f"خطأ 404: الرابط غير موجود. تأكد من أن webhook مُفعل في n8n وأن الرابط صحيح."

        response.raise_for_status()

        # Log successful response
        st.write("✅ Response received successfully")
        if response.text:
            st.write("Response body:", response.text)

        return True, "تم إرسال بيانات الخصم بنجاح!"

    except requests.exceptions.Timeout:
        return False, "خطأ: انتهت مهلة الاتصال. تحقق من اتصال الإنترنت."
    except requests.exceptions.ConnectionError:
        return False, "خطأ: فشل في الاتصال بالخادم. تحقق من الرابط."
    except requests.exceptions.HTTPError as e:
        return False, f"خطأ HTTP: {e}. كود الحالة: {e.response.status_code if e.response else 'غير معروف'}"
    except requests.exceptions.RequestException as e:
        return False, f"خطأ في إرسال بيانات الخصم: {str(e)}"


# Function to submit payment data
def submit_payment_data(payment_data):
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found for BigQuery access")
                return False, "Error: No credentials found"

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Prepare the query - insert into paid_showroom table
        query = """
        INSERT INTO `pricing-338819.wholesale_test.paid_showroom`
        (id, c_name, d_code, payment_date, payment_amount, date_of_payment, 
         sold_date, returned, return_date, request_id, submitted_by)
        VALUES
        (@id, @c_name, @d_code, @payment_date, @payment_amount, @date_of_payment,
         @sold_date, @returned, @return_date, @request_id, @submitted_by)
        """

        # Configure query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", payment_data['id']),
                bigquery.ScalarQueryParameter("c_name", "STRING", payment_data['c_name']),
                bigquery.ScalarQueryParameter("d_code", "STRING", payment_data['d_code']),
                bigquery.ScalarQueryParameter("payment_date", "DATE", payment_data['payment_date']),
                bigquery.ScalarQueryParameter("payment_amount", "NUMERIC", payment_data['payment_amount']),
                bigquery.ScalarQueryParameter("date_of_payment", "DATE", payment_data['date_of_payment']),
                bigquery.ScalarQueryParameter("sold_date", "DATE", payment_data['sold_date']),
                bigquery.ScalarQueryParameter("returned", "BOOL", payment_data['returned']),
                bigquery.ScalarQueryParameter("return_date", "DATE", payment_data['return_date']),
                bigquery.ScalarQueryParameter("request_id", "STRING", payment_data['request_id']),
                bigquery.ScalarQueryParameter("submitted_by", "STRING", payment_data['submitted_by'])
            ]
        )

        # Execute the query
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete

        return True, "تم تقديم بيانات الدفع بنجاح!"

    except Exception as e:
        return False, f"خطأ في تقديم بيانات الدفع: {str(e)}"


# Main app
def main():
    st.title("💰 نموذج بيانات الدفع")

    # Load data
    with st.spinner("جاري تحميل البيانات..."):
        dealers_data, dealers_dict = load_dealers()
        cars_data = load_car_names()
        discount_eligible_cars = load_discount_eligible_cars()
        discount_data = load_discount_data()

    if not dealers_data:
        st.warning("لا توجد بيانات متاحة للتجار.")
        return

    if not cars_data:
        st.warning("لا توجد بيانات متاحة للسيارات.")
        return

    # Create tabs for form and management
    tab1, tab2, tab3 = st.tabs(["📝 نموذج الدفع", "📊 إدارة السيارات المدفوعة", "🏷️ خصم المعرض"])

    with tab1:
        # Generate random ID
        random_id = str(uuid.uuid4())

        # Create form
        with st.form("payment_form"):
            st.subheader("بيانات الدفع")

            # Show generated ID
            st.info(f"معرف الدفعة: {random_id}")

            col1, col2 = st.columns(2)

            with col1:
                # Car name dropdown with details
                car_options = [
                    (car['sf_vehicle_name'], f"{car['sf_vehicle_name']} - {car['make']} {car['model']} ({car['year']})")
                    for car in cars_data]
                car_codes = [code for code, _ in car_options]
                car_displays = [display for _, display in car_options]

                selected_car_index = st.selectbox(
                    "اسم العميل",
                    options=range(len(car_options)),
                    format_func=lambda i: car_displays[i]
                )
                selected_car_name = car_codes[selected_car_index]

                # Payment amount
                payment_amount = st.number_input(
                    "مبلغ الدفع",
                    min_value=0.0,
                    step=100.0,
                    format="%.2f"
                )

            with col2:
                # Dealer selection
                dealer_options = [(dealer['dealer_code'], dealer['dealer_name']) for dealer in dealers_data]
                dealer_codes = [code for code, _ in dealer_options]
                dealer_names = [name for _, name in dealer_options]

                selected_dealer_index = st.selectbox(
                    "كود التاجر",
                    options=range(len(dealer_options)),
                    format_func=lambda i: f"{dealer_options[i][0]} - {dealer_options[i][1]}"
                )
                selected_dealer_code = dealer_codes[selected_dealer_index]

                # Date of payment (single date field)
                date_of_payment = st.date_input(
                    "تاريخ الدفع",
                    value=datetime.now().date()
                )

                # Submitter selection
                submitter_options = ["Nawal Mostafa", "Mai Yousif", "Mamdouh", "test"]
                submitted_by = st.selectbox(
                    "المرسل",
                    options=submitter_options
                )

            # Submit button
            submit_button = st.form_submit_button("إرسال بيانات الدفع", use_container_width=True)

            if submit_button:
                # Validate required fields
                if not payment_amount or payment_amount <= 0:
                    st.error("يرجى إدخال مبلغ دفع صحيح")
                    return

                # Prepare payment data
                payment_data = {
                    'id': random_id,
                    'c_name': selected_car_name,
                    'd_code': selected_dealer_code,
                    'payment_date': date_of_payment,
                    'payment_amount': payment_amount,
                    'date_of_payment': date_of_payment,
                    'sold_date': None,  # Left blank as requested
                    'returned': None,  # Left blank as requested
                    'return_date': None,  # Left blank as requested
                    'request_id': None,  # Left blank as requested
                    'submitted_by': submitted_by
                }

                # Submit payment data
                success, message = submit_payment_data(payment_data)

                if success:
                    st.success(message)
                    st.balloons()

                    # Send HTTP request to webhook for payment
                    try:
                        webhook_url = "https://anasalaa.app.n8n.cloud/webhook/e4ddbc51-cbb1-4cff-b88a-1062a3ab2cc7"
                        webhook_payload = {
                            "id": payment_data['id'],
                            "c_name": payment_data['c_name'],
                            "d_code": payment_data['d_code'],
                            "payment_date": str(payment_data['payment_date']),
                            "payment_amount": float(payment_data['payment_amount']),
                            "date_of_payment": str(payment_data['date_of_payment']),
                            "submitted_by": payment_data['submitted_by'],
                            "communication_type": "paid"
                        }

                        response = requests.post(webhook_url, json=webhook_payload)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        st.warning(f"Payment recorded successfully, but webhook notification failed: {str(e)}")

                    # Show submitted data for confirmation
                    with st.expander("البيانات المرسلة"):
                        st.json({
                            "معرف الدفعة": payment_data['id'],
                            "اسم العميل": payment_data['c_name'],
                            "كود التاجر": payment_data['d_code'],
                            "مبلغ الدفع": float(payment_data['payment_amount']),
                            "تاريخ الدفع": str(payment_data['date_of_payment']),
                            "المرسل": payment_data['submitted_by']
                        })
                else:
                    st.error(message)

    with tab2:
        st.subheader("💰 إدارة السيارات المدفوعة")

        # Query to get cars with no sold_date or return_date
        paid_cars_query = """
        SELECT 
            id,
            c_name,
            d_code,
            payment_date,
            payment_amount,
            date_of_payment,
            sold_date,
            returned,
            return_date,
            request_id,
            submitted_by
        FROM `pricing-338819.wholesale_test.paid_showroom`
        WHERE sold_date IS NULL AND return_date IS NULL
        ORDER BY payment_date DESC
        """

        try:
            # Get credentials and create client
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["service_account"]
                )
            except (KeyError, FileNotFoundError):
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        'service_account.json'
                    )
                except FileNotFoundError:
                    st.error("No credentials found for BigQuery access")
                    credentials = None

            if credentials:
                client = bigquery.Client(credentials=credentials)
                paid_cars_df = client.query(paid_cars_query).to_dataframe()

                if not paid_cars_df.empty:
                    # Display summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("السيارات المعلقة", len(paid_cars_df))
                    with col2:
                        total_amount = paid_cars_df['payment_amount'].sum()
                        st.metric("إجمالي المبلغ", f"EGP {total_amount:,.0f}")
                    with col3:
                        unique_dealers = len(paid_cars_df['d_code'].unique())
                        st.metric("التجار الفريدين", unique_dealers)

                    # Display cars with action buttons
                    for _, car in paid_cars_df.iterrows():
                        with st.expander(
                                f"🚗 {car['c_name']} - التاجر: {car['d_code']} - المبلغ: EGP {car['payment_amount']:,.0f}",
                                expanded=False
                        ):
                            col1, col2, col3 = st.columns([2, 1, 1])

                            with col1:
                                st.write(f"**معرف الدفعة:** {car['id']}")
                                st.write(f"**اسم السيارة:** {car['c_name']}")
                                st.write(f"**كود التاجر:** {car['d_code']}")
                                st.write(f"**تاريخ الدفع:** {car['payment_date']}")
                                st.write(f"**مبلغ الدفع:** EGP {car['payment_amount']:,.0f}")
                                st.write(f"**المرسل:** {car.get('submitted_by', 'غير محدد')}")

                            with col2:
                                if st.button("✅ تم البيع", key=f"sold_{car['id']}"):
                                    # Update query for sold
                                    update_sold_query = """
                                    UPDATE `pricing-338819.wholesale_test.paid_showroom`
                                    SET sold_date = CURRENT_DATE()
                                    WHERE id = @car_id
                                    """

                                    job_config = bigquery.QueryJobConfig(
                                        query_parameters=[
                                            bigquery.ScalarQueryParameter("car_id", "STRING", car['id'])
                                        ]
                                    )

                                    try:
                                        query_job = client.query(update_sold_query, job_config=job_config)
                                        query_job.result()
                                        st.success(f"تم تحديد السيارة {car['c_name']} كمباعة!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"خطأ في تحديد السيارة كمباعة: {str(e)}")

                            with col3:
                                if st.button("🔄 تم الإرجاع", key=f"returned_{car['id']}"):
                                    # Update query for returned
                                    update_returned_query = """
                                    UPDATE `pricing-338819.wholesale_test.paid_showroom`
                                    SET returned = TRUE, return_date = CURRENT_DATE()
                                    WHERE id = @car_id
                                    """

                                    job_config = bigquery.QueryJobConfig(
                                        query_parameters=[
                                            bigquery.ScalarQueryParameter("car_id", "STRING", car['id'])
                                        ]
                                    )

                                    try:
                                        query_job = client.query(update_returned_query, job_config=job_config)
                                        query_job.result()

                                        # Send HTTP request to webhook for returned car BEFORE showing success message
                                        webhook_success = False
                                        try:
                                            webhook_url = "https://anasalaa.app.n8n.cloud/webhook/e4ddbc51-cbb1-4cff-b88a-1062a3ab2cc7"
                                            webhook_payload = {
                                                "id": str(car['id']),
                                                "c_name": str(car['c_name']),
                                                "d_code": str(car['d_code']),
                                                "payment_date": str(car['payment_date']),
                                                "payment_amount": float(car['payment_amount']),
                                                "date_of_payment": str(car['date_of_payment']),
                                                "return_date": str(datetime.now().date()),
                                                "returned": True,
                                                "communication_type": "returned"
                                            }

                                            response = requests.post(webhook_url, json=webhook_payload, timeout=10)
                                            response.raise_for_status()
                                            webhook_success = True
                                        except requests.exceptions.RequestException as e:
                                            st.error(f"Webhook error: {str(e)}")

                                        # Show success message
                                        if webhook_success:
                                            st.success(f"تم تحديد السيارة {car['c_name']} كمرتجعة وتم إرسال التنبيه!")
                                        else:
                                            st.success(f"تم تحديد السيارة {car['c_name']} كمرتجعة (تعذر إرسال التنبيه)")

                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"خطأ في تحديد السيارة كمرتجعة: {str(e)}")
                else:
                    st.info("�� لا توجد سيارات معلقة! جميع السيارات تم بيعها أو إرجاعها.")

                # Add a section to show completed transactions
                st.subheader("📊 المعاملات الأخيرة")

                # Query for recent completed transactions
                completed_query = """
                SELECT 
                    id,
                    c_name,
                    d_code,
                    payment_date,
                    payment_amount,
                    sold_date,
                    return_date,
                    submitted_by,
                    CASE 
                        WHEN sold_date IS NOT NULL THEN 'مباع'
                        WHEN return_date IS NOT NULL THEN 'مرتجع'
                        ELSE 'معلق'
                    END as status
                FROM `pricing-338819.wholesale_test.paid_showroom`
                WHERE sold_date IS NOT NULL OR return_date IS NOT NULL
                ORDER BY COALESCE(sold_date, return_date) DESC
                LIMIT 10
                """

                completed_df = client.query(completed_query).to_dataframe()

                if not completed_df.empty:
                    # Format the dataframe for display
                    display_df = completed_df.copy()

                    # Format dates
                    display_df['payment_date'] = pd.to_datetime(display_df['payment_date']).dt.strftime('%Y-%m-%d')
                    display_df['sold_date'] = pd.to_datetime(display_df['sold_date']).dt.strftime('%Y-%m-%d')
                    display_df['return_date'] = pd.to_datetime(display_df['return_date']).dt.strftime('%Y-%m-%d')

                    # Format payment amount
                    display_df['payment_amount'] = display_df['payment_amount'].apply(
                        lambda x: f"EGP {x:,.0f}" if pd.notnull(x) else "N/A"
                    )

                    st.dataframe(
                        display_df,
                        column_config={
                            "id": "معرف الدفعة",
                            "c_name": "اسم السيارة",
                            "d_code": "كود التاجر",
                            "payment_date": "تاريخ الدفع",
                            "payment_amount": "المبلغ",
                            "sold_date": "تاريخ البيع",
                            "return_date": "تاريخ الإرجاع",
                            "submitted_by": "المرسل",
                            "status": "الحالة"
                        },
                        use_container_width=True
                    )
                else:
                    st.info("لا توجد معاملات مكتملة حتى الآن.")

        except Exception as e:
            st.error(f"خطأ في تحميل بيانات المعرض المدفوع: {str(e)}")

    with tab3:
        st.subheader("🏷️ نموذج خصم المعرض")

        if not discount_eligible_cars:
            st.warning("لا توجد سيارات مؤهلة للخصم.")
            return

        if not discount_data:
            st.warning("لا توجد بيانات خصم متاحة.")
            return

        # Filter cars to only show those that are both discount eligible AND have discount data
        eligible_cars_with_discount = [
            car for car in discount_eligible_cars
            if any(d['c_code'] == car['sf_vehicle_name'] for d in discount_data)
        ]

        if not eligible_cars_with_discount:
            st.warning("لا توجد سيارات مؤهلة للخصم مع بيانات خصم متاحة.")
            return

        # Car selection dropdown - outside of form to allow dynamic updates
        st.subheader("اختيار السيارة والتاجر")

        col1, col2 = st.columns(2)

        with col1:
            # Car selection dropdown - only show cars that are both discount eligible AND have discount data
            car_options = [
                (car['sf_vehicle_name'], f"{car['sf_vehicle_name']}  {car['days_in_consignment']} ")
                for car in eligible_cars_with_discount
            ]
            car_codes = [code for code, _ in car_options]
            car_displays = [display for _, display in car_options]

            selected_car_index = st.selectbox(
                "اختر السيارة",
                options=range(len(car_options)),
                format_func=lambda i: car_displays[i],
                key="discount_car_select"
            )
            selected_car_code = car_codes[selected_car_index]
            selected_car_info = eligible_cars_with_discount[selected_car_index]

        with col2:
            # Dealer selection
            dealer_options = [(dealer['dealer_code'], dealer['dealer_name']) for dealer in dealers_data]
            dealer_codes = [code for code, _ in dealer_options]

            selected_dealer_index = st.selectbox(
                "اختر التاجر",
                options=range(len(dealer_options)),
                format_func=lambda i: f"{dealer_options[i][0]} - {dealer_options[i][1]}",
                key="discount_dealer_select"
            )
            selected_dealer_code = dealer_codes[selected_dealer_index]

        # Find discount data for selected car
        car_discount = next((d for d in discount_data if d['c_code'] == selected_car_code), None)

        if car_discount:
            st.subheader("معلومات الخصم")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "السعر السريع",
                    f"EGP {car_discount['speed_discount_price']:,.0f}" if car_discount[
                        'speed_discount_price'] else "غير متاح"
                )

            with col2:
                st.metric(
                    "سعر الاستلام",
                    f"EGP {car_discount['consignment_price']:,.0f}" if car_discount['consignment_price'] else "غير متاح"
                )

            with col3:
                if car_discount['speed_discount_price'] and car_discount['consignment_price']:
                    discount_amount = car_discount['consignment_price'] - car_discount['speed_discount_price']
                    st.metric("مبلغ الخصم", f"EGP {discount_amount:,.0f}")
                else:
                    st.metric("مبلغ الخصم", "غير متاح")

            with col4:
                st.metric(
                    "السعر الفوري",
                    f"EGP {car_discount['flash_price']:,.0f}" if car_discount['flash_price'] else "غير متاح"
                )

            # Car details
            st.subheader("تفاصيل السيارة")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.info(f"**أيام الاستلام:** {selected_car_info['days_in_consignment']}")

            with col2:
                st.info(f"**عدد مرات العرض:** {selected_car_info['showroom_displayed_count']}")

            with col3:
                st.info(f"**عدد الطوابير:** {selected_car_info['queue_count']}")

            # Create form for submission
            with st.form("discount_form"):
                st.subheader("تأكيد إرسال بيانات الخصم")

                # Show summary of what will be sent
                st.info(f"**السيارة المختارة:** {selected_car_code}")
                st.info(f"**التاجر المختار:** {selected_dealer_code}")

                # Submit button
                submit_discount = st.form_submit_button("إرسال بيانات الخصم", use_container_width=True)

                if submit_discount:
                    # Prepare discount payload
                    discount_payload = {
                        "c_code": selected_car_code,
                        "dealer_code": selected_dealer_code,
                        "flash_price": float(car_discount['flash_price']) if car_discount['flash_price'] else 0.0,
                        "consignment_price": float(car_discount['consignment_price']) if car_discount[
                            'consignment_price'] else 0.0,
                        "speed_discount_price": float(car_discount['speed_discount_price']) if car_discount[
                            'speed_discount_price'] else 0.0,
                        "days_in_consignment": selected_car_info['days_in_consignment'],
                        "showroom_displayed_count": selected_car_info['showroom_displayed_count'],
                        "queue_count": selected_car_info['queue_count'],
                        "car_status": selected_car_info['car_status']
                    }

                    # Submit to webhook
                    success, message = submit_discount_data(discount_payload)

                    if success:
                        st.success(message)
                        st.balloons()

                        # Show submitted data for confirmation
                        with st.expander("البيانات المرسلة"):
                            st.json({
                                "كود السيارة": discount_payload['c_code'],
                                "كود التاجر": discount_payload['dealer_code'],
                                "السعر الفوري": discount_payload['flash_price'],
                                "سعر الاستلام": discount_payload['consignment_price'],
                                "السعر السريع": discount_payload['speed_discount_price'],
                                "أيام الاستلام": discount_payload['days_in_consignment'],
                                "عدد مرات العرض": discount_payload['showroom_displayed_count'],
                                "عدد الطوابير": discount_payload['queue_count'],
                                "حالة السيارة": discount_payload['car_status']
                            })
                    else:
                        st.error(message)

        else:
            st.error(f"لا توجد بيانات خصم للسيارة {selected_car_code}")


if __name__ == "__main__":
    # Set Arabic RTL layout
    st.markdown("""
    <style>
    body {
        direction: rtl;
        text-align: right;
    }
    .stTextInput, .stTextArea, .stSelectbox, .stMultiselect, .stNumberInput {
        direction: rtl;
        text-align: right;
    }
    </style>
    """, unsafe_allow_html=True)

    main()
