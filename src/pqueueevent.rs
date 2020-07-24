use crate::page::Page;
use crate::job::Job;

enum PQueueEvent {
    ReadResponse(usize, Page),
    WriteResponse(usize),
    IncRequest(Job),
    PopRequest,
}
