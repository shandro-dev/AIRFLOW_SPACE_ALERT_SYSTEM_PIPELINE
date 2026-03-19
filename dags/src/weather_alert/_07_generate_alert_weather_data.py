# GENERATE ALERT

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os
load_dotenv()

def send_weather_threat_alert_email(alerts, recipient_email, sender_email, sender_password):
    if not alerts:
        print("✅ No extreme alerts to send.")
        return

    subject = "IMPORTANT ALERT: Severe Weather Conditions Reported"


    # Build HTML body with minimal styling
    html_body = """
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="border-bottom: 4px solid red; padding-bottom: 10px; margin-bottom: 20px;">
          <h1 style="color: red; font-size: 20px; margin: 0;">⚠️ Extreme Weather Alert Report</h1>
        </div>
        <p>Dear User,</p>
        <p>This is an important weather alert from our monitoring system. The following major cities are currently experiencing extreme weather conditions:</p>
        
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
          <thead>
            <tr style="background-color: #f2f2f2;">
              <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">City</th>
              <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Conditions</th>
            </tr>
          </thead>
          <tbody>
    """

    for alert in alerts:
        city = alert['city']
        events = ", ".join(alert['events'])
        html_body += f"""
            <tr>
              <td style="border: 1px solid #ddd; padding: 8px;">{city}</td>
              <td style="border: 1px solid #ddd; padding: 8px;">{events}</td>
            </tr>
        """

    html_body += """
          </tbody>
        </table>

        <p>Please take necessary precautions.</p>
        <p><em>This is an automated notification. Please do not reply to this message.</em></p><br>
        <p>Regards,<br>Weather Alert System</p>
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

    # Attach plain text fallback (optional but good practice)
    plain_body = "Dear User,\n\nThis is an important weather alert from our monitoring system.\n\n"
    for alert in alerts:
        city = alert['city']
        events = ", ".join(alert['events'])
        plain_body += f"{city}: {events}\n"
    plain_body += "\nPlease take necessary precautions.\nThis is an automated notification. Please do not reply.\n\nRegards,\nWeather Alert System"
    
    message.attach(MIMEText(plain_body, 'plain'))
    message.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print(f"✅ Weather Alert email sent to {recipient_email}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")