import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_pipeline_halted_email():
    """Notifies the team that the pipeline was stopped due to validation failure."""
    
    sender_email = os.getenv("SENDER_MAIL_ID")
    sender_password = os.getenv("GMAIL_APP_PASSWORD")
    recipient_emails = os.getenv("RECIPIENT_EMAILS", "")
    # for admins
    # recipient_emails=os.getenv("ADMIN_RECIPIENT_EMAILS", "")
    recipient_list = [e.strip() for e in recipient_emails.split(",") if e.strip()]

    if not sender_email or not sender_password:
        print("❌ Email credentials missing. Skipping halt notification.")
        return

    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_list)
    msg['Subject'] = "CRITICAL: NASA Pipeline Execution Halted"

    # --- HTML Body ---
    html_content = f"""
    <html>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 20px; border-radius: 8px; border-top: 6px solid #b91c1c;">
                <h2 style="color: #b91c1c; margin-top: 0;">Pipeline Execution Halted</h2>
                <p>Hello,</p>
                
                <p>This is a formal notification that the <strong>NASA Near-Earth Object Pipeline</strong> has been terminated prematurely.</p>
                
                <div style="background-color: #fef2f2; border: 1px solid #fee2e2; padding: 15px; border-radius: 4px; color: #991b1b;">
                    <strong>Reason for Halt:</strong> Data Validation Check Failed.<br>
                    <strong>Security Measure:</strong> The system has blocked the data-loading phase to ensure database integrity and prevent the ingestion of malformed or invalid records.
                </div>

                <p>Immediate action is required to inspect the <code>validate_neo_data</code> task logs and rectify the data discrepancies before the next scheduled run.</p>

                <p style="margin-top: 30px;">Regards,<br><strong>Space Alert Automation System</strong></p>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="font-size: 11px; color: #888; text-align: center;">
                    <strong>Notice:</strong> This is a system-generated automated mail. Please do not reply.
                </p>
            </div>
        </body>
    </html>
    """

    msg.add_alternative(html_content, subtype='html')

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        print("✅ Pipeline Halted email sent.")
    except Exception as e:
        print(f"❌ Failed to send halt email: {e}")