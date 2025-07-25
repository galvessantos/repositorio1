package com.montreal.core.domain.service;

import com.montreal.broker.components.SgdBrokerComponent;
import com.montreal.broker.service.SgdBrokerService;
import com.montreal.core.domain.component.EmailComponent;
import com.montreal.core.domain.exception.ClientServiceException;
import com.montreal.core.domain.exception.EmailException;
import com.montreal.core.properties.UiHubProperties;
import com.montreal.oauth.domain.entity.UserInfo;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import javax.mail.MessagingException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmailService {

    private final JavaMailSender mailSender;
    private final SgdBrokerComponent sgdBrokerComponent;
    private final SgdBrokerService sgdBrokerService;
    private final UiHubProperties uiHubProperties;

    public void sendPasswordResetEmail(String to, String token) {
        String resetUrl = "http://localhost:8082/api/v1/auth/reset-password?token=" + token;

        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(to);
        message.setSubject("Redefinição de Senha");
        message.setText("Para redefinir sua senha, clique no link abaixo:\n" + resetUrl);

        mailSender.send(message);
    }

    public String getTamplate(String templateName, String name, String link) {

        StringBuilder headBuilder = new StringBuilder();
        StringBuilder footerBuilder = new StringBuilder();
        StringBuilder contentBuilder = new StringBuilder();

        try {

            BufferedReader inHead = new BufferedReader(new FileReader("C:\\PROJETO\\RELEASE\\MONTREAL-GESTAO-GARANTIAS-BACKEND\\src\\main\\java\\com\\montreal\\core\\templates\\shared\\head.html"));
            BufferedReader inContent = new BufferedReader(new FileReader("C:\\PROJETO\\RELEASE\\MONTREAL-GESTAO-GARANTIAS-BACKEND\\src\\main\\java\\com\\montreal\\core\\templates\\" + templateName + ".html"));
            BufferedReader inFooter = new BufferedReader(new FileReader("C:\\PROJETO\\RELEASE\\MONTREAL-GESTAO-GARANTIAS-BACKEND\\src\\main\\java\\com\\montreal\\core\\templates\\shared\\footer.html"));
            String strHead;
            String strFooter;
            String strContent;
            while ((strHead = inHead.readLine()) != null) {
                headBuilder.append(strHead);
            }
            while ((strContent = inContent.readLine()) != null) {
                contentBuilder.append(strContent);
            }
            while ((strFooter = inFooter.readLine()) != null) {
                footerBuilder.append(strFooter);
            }
            inHead.close();
            inContent.close();
            inFooter.close();

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String head = headBuilder.toString();
        String footer = footerBuilder.toString();

        String content = contentBuilder.toString();
        content = content.replace("${HEAD}", head);
        content = content.replace("${FOOTER}", footer);
        content = content.replace("${NAME}", name);
        content = content.replace("${LINK}", link);
        return content;
    }

    public void sendEmailFromTemplate(String name, String link, String recipient) throws MessagingException {
        MimeMessage message = this.mailSender.createMimeMessage();
        String template = getTamplate("forgot-password", name, link);
        String subject = "Recuperação de senha";
        String from = "suporte@montreal.com.br";
        Map<String, Object> variables = new HashMap<>();
        variables.put("NAME", "WELL");
        variables.put("LINK", link);

        try {
            message.setSubject(subject);
            message.setContent(template, "text/html; charset=utf-8");
            message.setRecipients(MimeMessage.RecipientType.TO, recipient);
            message.setFrom(from);
            mailSender.send(message);
        } catch (jakarta.mail.MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendEmailRegistrationConfirmation(UserInfo userInfo) {
        log.info("Enviando e-mail de confirmação de cadastro para {}", userInfo.getEmail());
        try {
            var linkRegister = uiHubProperties.getUrl() + "cadastrarSenha/" + userInfo.getId();
            var linkLogin = uiHubProperties.getUrl() + "login/";
            var template = EmailComponent.getTemplateEmailNewUser(userInfo.getUsername(), userInfo.getFullName(), linkRegister, linkLogin);
            var digitalSendRequest = sgdBrokerComponent.createTypeEmail("Confirmação de Cadastro", template, userInfo.getEmail());
            var digitalSendResponse = sgdBrokerService.sendNotification(digitalSendRequest);
            log.info("E-mail de confirmação de cadastro enviado com sucesso para {} - código envio: {}", userInfo.getEmail(), digitalSendResponse.getSendId());
        } catch (ClientServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new EmailException("Erro ao enviar e-mail de confirmação de cadastro", e);
        }
    }
}
