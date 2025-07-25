package com.montreal.core.domain.component;

import lombok.experimental.UtilityClass;

@UtilityClass
public class EmailComponent {

    public static String getTemplateEmailNewUser(String username, String name, String linkCreatePassword, String linkAccessSystem) {

        var template = """
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <h2>Caro(a) {{NAME}},</h2>
                <p>Seu cadastro no sistema <b>hubRecupera</b> foi efetuado com sucesso!</p>
                <p>Seu login de acesso é: <b>{{USERNAME}}</b></p>
                <p>
                    Para cadastrar sua senha de acesso,
                    <a href={{LINK_CREATE_PASSWORD}}
                       style="color: #4CAF50; text-decoration: none; font-weight: bold;">
                       clique aqui
                    </a>.
                </p>
                <p>Lembrando que sua senha deve atender aos seguintes critérios:</p>
                <ul>
                    <li><b>Tamanho:</b> Entre 4 e 8 caracteres.</li>
                    <li><b>Letras:</b> Inclua pelo menos uma letra maiúscula e uma letra minúscula.</li>
                    <li><b>Números:</b> Adicione pelo menos um número.</li>
                    <li><b>Caracteres especiais:</b> pelo menos um. Aceitos: <code>_</code> (sublinhado), <code>@</code> (arroba) e <code>#</code> (tralha).</li>
                </ul>
                <p>
                    Após cadastrar sua senha,
                    <a href={{LINK_ACCESS_SYSTEM}}
                       style="color: #4CAF50; text-decoration: none; font-weight: bold;">
                       clique aqui
                    </a> para acessar o sistema.
                </p>
                <p>Atenciosamente,<br>hubRecupera</p>
            </body>
        </html>
        """;

        return template.replace("{{NAME}}", name)
                .replace("{{USERNAME}}", username)
                .replace("{{LINK_CREATE_PASSWORD}}", linkCreatePassword)
                .replace("{{LINK_ACCESS_SYSTEM}}", linkAccessSystem);
    }
}
