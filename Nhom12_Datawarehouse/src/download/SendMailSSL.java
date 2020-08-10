package download;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendMailSSL {
	
	Properties props = null;
	
	
	
	
	public SendMailSSL() {
		// Lay doi tuong cua mail
		this.props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", MailConfig.HOST_NAME); 
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.port", MailConfig.TSL_PORT);;
	}




	public void sendMail(String Tieude,String noiDung){
        // lay Session cua mail
        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(MailConfig.APP_EMAIL, MailConfig.APP_PASSWORD);
            }
        });
 
        // qua trinh soan tin
        try {
            MimeMessage message = new MimeMessage(session);
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(MailConfig.RECEIVE_EMAIL));
            message.setSubject(Tieude);
            message.setText(noiDung);
             
            // gui mail
            Transport.send(message);
             
            System.out.println("Message sent successfully");
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
	}
}
