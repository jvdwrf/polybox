use super::*;
use std::any::TypeId;
use type_sets::Members;

pub trait Interface: Message<Kind = FireAndForget> + AsSet + Sized + Send + 'static {
    fn try_from_any_payload(payload: AnyPayload) -> Result<Self, AnyPayload>;
    fn into_any_payload(self) -> AnyPayload;

    fn try_from_payload<I: Message>(payload: Payload<I>) -> Result<Self, Payload<I>>
    where
        Payload<I>: Send,
    {
        // This can be implemented faster using unsafe transmute
        Self::try_from_any_payload(AnyPayload::new::<I>(payload))
            .map_err(|payload| payload.downcast::<I>().expect("Conversion back"))
    }

    fn try_into_payload<I: Message>(self) -> Result<Payload<I>, Self> {
        // This can be implemented faster using unsafe transmute
        self.into_any_payload()
            .downcast::<I>()
            .map_err(|payload| Self::try_from_any_payload(payload).expect("Conversion back"))
    }

    fn invocable_with(type_id: TypeId) -> bool {
        Self::members().contains(&type_id)
    }
}
