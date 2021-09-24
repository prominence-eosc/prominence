import configparser
import smtplib
from email.message import EmailMessage

CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

def send_email(email_address, identity, subject, content):
    msg = EmailMessage()
    msg.set_content(content)

    msg['Subject'] = subject
    msg['From'] = CONFIG.get('email', 'from')
    msg['To'] = email_address

    try:
        s = smtplib.SMTP(CONFIG.get('email', 'server'))
        s.send_message(msg)
        s.quit()
    except:
        pass
