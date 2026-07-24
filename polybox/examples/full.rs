use polybox::{
    DynInbox, Interface, Message, Payload, PolyboxExt as _, Sends, SendsExt as _,
    inboxes::{FlumeInbox, TokioInbox},
    type_sets::Set,
};

// The following are messages defined for the NumberAdder and Printer actors.
// Some messages have replies, while others are fire-and-forget.
//
// The Health and Exit messages are accepted by both actors, whilst the others
// are specific to each actor.

#[derive(Message, Debug)]
#[msg(reply = Health)]
pub struct GetHealth;

#[derive(Debug)]
pub enum Health {
    Positive,
    Negative,
}

#[derive(Message, Debug)]
pub struct Exit;

#[derive(Message, Debug)]
pub struct AddNumber(u32);

#[derive(Message, Debug)]
#[msg(reply = u32)]
pub struct GetNumber;

#[derive(Message, Debug)]
pub struct Print(&'static str);

/// A simple actor that adds numbers and can report its total.
#[derive(Interface, Debug)]
pub enum NumberAdder {
    Health(Payload<GetHealth>),
    Exit(Payload<Exit>),
    Add(Payload<AddNumber>),
    Get(Payload<GetNumber>),
}

impl NumberAdder {
    fn spawn() -> (TokioInbox<NumberAdder>, tokio::task::JoinHandle<()>) {
        let (inbox, mut receiver) = TokioInbox::<NumberAdder>::new(1000);

        let handle = tokio::spawn(async move {
            let mut total: u32 = 0;

            while let Some(msg) = receiver.recv().await {
                match msg {
                    NumberAdder::Health((GetHealth, tx)) => {
                        let _ = tx.send(Health::Positive);
                    }
                    NumberAdder::Exit(Exit) => {
                        break;
                    }
                    NumberAdder::Add(payload) => {
                        total += payload.0;
                    }
                    NumberAdder::Get((GetNumber, tx)) => {
                        let _ = tx.send(total);
                    }
                }
            }
        });

        (inbox, handle)
    }
}

/// A simple actor that prints messages.
#[derive(Interface, Debug)]
pub enum Printer {
    Health(Payload<GetHealth>),
    Exit(Payload<Exit>),
    Print(Payload<Print>),
}

impl Printer {
    fn spawn() -> (FlumeInbox<Printer>, tokio::task::JoinHandle<()>) {
        let (inbox, receiver) = FlumeInbox::<Printer>::new(1000);

        let handle = tokio::spawn(async move {
            while let Ok(msg) = receiver.recv_async().await {
                match msg {
                    Printer::Health((GetHealth, tx)) => {
                        let _ = tx.send(Health::Positive);
                    }
                    Printer::Exit(Exit) => {
                        break;
                    }
                    Printer::Print(payload) => {
                        println!("Printer received: {}", payload.0);
                    }
                }
            }
        });

        (inbox, handle)
    }
}

#[tokio::main]
pub async fn main() {
    let (adder, adder_handle) = NumberAdder::spawn();
    let (printer, printer_handle) = Printer::spawn();

    // Convert the individual inboxes into their common subset.
    // This even converts a FlumeInbox and TokioInbox into a common type.
    let all_inboxes: Vec<DynInbox<Set![Exit, GetHealth]>> = vec![
        adder.clone().into_dyn_subset(),
        printer.clone().into_dyn_subset(),
    ];

    // Start a background task to monitor the health of all inboxes.
    tokio::task::spawn({
        let all_inboxes = all_inboxes.clone();
        async move {
            monitor_inboxes_in_background(&all_inboxes).await;
        }
    });

    // Send some messages to the actors and check their responses.
    adder.send(AddNumber(10)).await.unwrap();
    adder.send(AddNumber(20)).await.unwrap();
    let number = adder.request(GetNumber).await.unwrap();
    assert_eq!(number, 30);
    printer.send(Print("Hello!")).await.unwrap();

    // Wait for a moment to let the actors process the messages before exiting.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    send_exit_to(&all_inboxes).await;

    adder_handle.await.unwrap();
    printer_handle.await.unwrap();
}

/// A helper function to monitor the health of multiple inboxes in the background.
pub async fn monitor_inboxes_in_background(inboxes: &[impl Sends<GetHealth>]) {
    loop {
        for inbox in inboxes {
            let health = inbox.request(GetHealth).await.unwrap();

            match health {
                Health::Positive => {
                    println!("Inbox is healthy");
                }
                Health::Negative => {
                    println!("Inbox is unhealthy");
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

pub async fn send_exit_to(inboxes: &[impl Sends<Exit>]) {
    for inbox in inboxes {
        inbox.send(Exit).await.unwrap();
    }
}
