class ErroPipeline(Exception):
    """Falha esperada (dado ou arquivo), não bug: a CLI mostra só a mensagem."""


class FonteNaoEncontrada(ErroPipeline):
    pass


class LayoutInvalido(ErroPipeline):
    pass
