# GENERATE ALERT

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os
load_dotenv()

def send_asteroid_threat_alert_email(alerts, recipient_email, sender_email, sender_password):
    if not alerts:
        print("✅ No asteroid threats to send.")
        return

    subject = "CRITICAL ALERT: Potentially Hazardous Asteroids Detected"

    # Build HTML body with minimal styling
    html_body = """
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="border-bottom: 4px solid red; padding-bottom: 10px; margin-bottom: 20px;">
          <h1 style="color: red; font-size: 20px; margin: 0;">⚠️ Near-Earth Object Alert</h1>
        </div>
        <p>Dear User,</p>
        <p>The following asteroids have been flagged as potential threats by our monitoring system:</p>

        <table style="border-collapse: collapse; width: 100%; max-width: 800px;">
          <thead>
            <tr style="background-color: #f2f2f2;">
              <th style="border: 1px solid #ddd; padding: 8px;">Asteroid Name</th>
              <th style="border: 1px solid #ddd; padding: 8px;">Closest Approach (IST)</th>
              <th style="border: 1px solid #ddd; padding: 8px;">Alerts</th>
              <th style="border: 1px solid #ddd; padding: 8px;">NASA URL</th>
            </tr>
          </thead>
          <tbody>
    """

    for alert in alerts:
        name = alert['asteroid_name']
        time = alert['closest_approach_time_to_earth_IST']
        events = ", ".join(alert['alerts'])
        url = alert['url']
        html_body += f"""
            <tr>
              <td style="border: 1px solid #ddd; padding: 8px;">{name}</td>
              <td style="border: 1px solid #ddd; padding: 8px;">{time}</td>
              <td style="border: 1px solid #ddd; padding: 8px;">{events}</td>
              <td style="border: 1px solid #ddd; padding: 8px;"><a href="{url}" target="_blank">Link</a></td>
            </tr>
        """

    html_body += """
          </tbody>
        </table>

        <p>Please stay informed and follow official space agency bulletins.</p>
        <p><em>This is an automated alert. Do not reply to this email.</em></p><br>
        <p>Regards,<br>Asteroid Alert System</p>
      </body>
    </html>
    """

    message = MIMEMultipart("alternative")
    message['From'] = sender_email
    message['To'] = ", ".join(recipient_email)
    message['Subject'] = subject
    message['X-Priority'] = '1'
    message['X-MSMail-Priority'] = 'High'
    message['Importance'] = 'High'
    message['Priority'] = 'urgent'

    # Plain text fallback
    plain_body = "Dear User,\n\nThe following asteroids pose potential threats:\n\n"
    for alert in alerts:
        plain_body += f"{alert['asteroid_name']} - {alert['closest_approach_time_to_earth_IST']}: {', '.join(alert['alerts'])}\nLink: {alert['url']}\n"
    plain_body += "\nPlease stay informed.\nDo not reply to this message.\n\nRegards,\nAsteroid Alert System"

    message.attach(MIMEText(plain_body, 'plain'))
    message.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print(f"✅ Space Alert email sent to {recipient_email}")
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
      


# def run_space_alert_system():
#     alerts = detect_near_earth_threats(df)   
#     recipient_emails = os.getenv("RECIPIENT_EMAILS", "")
#     recipient_list = [email.strip() for email in recipient_emails.split(",") if email.strip()]
#     send_asteroid_threat_alert_email(
#         alerts,
#         recipient_email=recipient_list,
#         sender_email=os.getenv("SENDER_MAIL_ID"),
#         sender_password=os.getenv("GMAIL_APP_PASSWORD")
#     )

