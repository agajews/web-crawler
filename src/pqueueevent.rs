use crate::page::Page;
use crate::job::Job;

pub enum PQueueEvent {
    ReadResponse(usize, Page),
    WriteResponse(usize),
    IncRequest(Job),
    PopRequest,
}
