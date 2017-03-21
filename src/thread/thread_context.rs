use std::cell::RefCell;
use std::fmt::Debug;
use std::clone::Clone;

const EMPTY_THREAD_ID: usize = 0;

#[derive(Debug,Clone)]
pub struct ThreadContext {
  pub thread_id: usize
}

thread_local!{
    static THREAD_CONTEXT: RefCell<ThreadContext> = RefCell::new(ThreadContext::new(EMPTY_THREAD_ID));
}



impl ThreadContext {
  pub fn new(id: usize) -> ThreadContext {
    ThreadContext { thread_id: id }
  }

  pub fn current() -> ThreadContext {
    let this_thread_context = THREAD_CONTEXT.with(|x| x.borrow().clone());
    assert_ne!(EMPTY_THREAD_ID, this_thread_context.thread_id);
    this_thread_context
  }

  pub fn set_current(context: ThreadContext) {
    THREAD_CONTEXT.with(|cur_cxt| {
        *(cur_cxt.borrow_mut()) = context;
    });
  }
}

#[cfg(test)]
mod tests {
  use super::ThreadContext;

  #[test]
  fn test_init() {
    let cur_cxt = ThreadContext::new(4) ;
    ThreadContext::set_current(cur_cxt);

    assert_eq!(4, ThreadContext::current().thread_id);
  }

  #[test]
  #[should_panic]
  fn test_uninited() {
    ThreadContext::current();
  }
}


