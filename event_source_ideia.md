Na verdade, eu faria um "Event Source"

Eu abstrairia ainda mais.

Em vez de pensar em "Timer", "io_uring" e "Mailbox", pensaria em fontes de eventos.

Todos implementam algo como:

trait EventSource {
    fn poll(&mut self, events: &mut EventQueue);
}

O runtime teria:

IoUringSource

TimerSource

MailboxSource

SimulationSource

SignalSource

Todos apenas colocam (Handle, Message) na mesma fila de eventos.

Então o loop principal fica extremamente uniforme:

for source in event_sources {
    source.poll(&mut ready_queue);
}

while let Some((handle, msg)) = ready_queue.pop() {
    isolate.handle(msg);
}

Isso significa que adicionar uma nova funcionalidade — sinais do sistema, watchdogs, métricas, até uma interface para testes determinísticos — não exige mudar o scheduler. Basta implementar outra EventSource. Na minha opinião, essa é uma abstração ainda mais elegante do que tratar timers como um caso especial: o scheduler só consome eventos; ele não se importa de onde eles vieram.