use crate::page::Page;
use crate::job::Job;

pub enum PQueueEvent {
    ReadResponse(usize, Page),
    IncRequest(Job),
    PopRequest,
}
