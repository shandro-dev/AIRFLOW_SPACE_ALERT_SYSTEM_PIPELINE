import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_load_success_email(is_skipped, is_failure, schema_name, table_name, batch_id, record_count):
    sender_email = os.getenv("SENDER_MAIL_ID")
    sender_password = os.getenv("GMAIL_APP_PASSWORD")
    recipient_emails = os.getenv("RECIPIENT_EMAILS", "")
    # for admins
    # recipient_emails=os.getenv("ADMIN_RECIPIENT_EMAILS", "")
    
    recipient_list = [e.strip() for e in recipient_emails.split(",") if e.strip()]

    if not sender_email or not sender_password:
        print("❌ Email credentials missing. Skipping notification.")
        return

    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_list)

    # --- Determine Status and Design Colors ---
    if is_failure == 'yes':
        subject_icon = "❌"
        status_title = "Data Load Failed"
        status_color = "#d9534f"  # Red
        status_msg = "The automated data load process encountered an error and could not complete."
    elif is_skipped == 'yes':
        subject_icon = "🟡"
        status_title = "Data Load Skipped"
        status_color = "#f0ad4e"  # Orange
        status_msg = "The load process was skipped. Records already exist in the database for this period."
    else:
        subject_icon = "✅"
        status_title = "Data Load Success"
        status_color = "#5cb85c"  # Green
        status_msg = "The NASA Near-Earth Object pipeline has successfully processed and loaded the data."

    msg['Subject'] = f"{subject_icon} NASA Pipeline System Notification: {status_title}"

    # --- HTML Body for Professional Design ---
    html_content = f"""
    <html>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #eee; padding: 20px; border-radius: 10px;">
                <h2 style="color: {status_color}; border-bottom: 2px solid {status_color}; padding-bottom: 10px;">
                    {status_title}
                </h2>
                <p>Hello,</p>
                <p>{status_msg}</p>
                
                {"<table style='width: 100%; border-collapse: collapse; margin: 20px 0;'> "
                 "<tr><td style='padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;'>Schema</td>"
                 f"<td style='padding: 8px; border: 1px solid #ddd;'>{schema_name}</td></tr>"
                 "<tr><td style='padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;'>Table</td>"
                 f"<td style='padding: 8px; border: 1px solid #ddd;'>{table_name}</td></tr>"
                 "<tr><td style='padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;'>Batch ID</td>"
                 f"<td style='padding: 8px; border: 1px solid #ddd;'>{batch_id}</td></tr>"
                 "<tr><td style='padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;'>Records</td>"
                 f"<td style='padding: 8px; border: 1px solid #ddd;'>{record_count}</td></tr>"
                 "</table>" if is_failure == 'no' and is_skipped == 'no' else ""}

                <p style="margin-top: 30px;">Regards,<br><strong>Space Alert Automation System</strong></p>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="font-size: 11px; color: #888; text-align: center;">
                    This is an automated system-generated email. Please do not reply to this message.
                </p>
            </div>
        </body>
    </html>
    """

    # Add HTML content
    msg.add_alternative(html_content, subtype='html')

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        print(f"✅ Professional email sent ({status_title}).")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")