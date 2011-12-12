# Copyright (c) 2005 Jamis Buck
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
module DeadlockRetry

  # How many retries should a query get before finally giving up?
  mattr_accessor :maximum_retries_on_deadlock
  self.maximum_retries_on_deadlock = 3

  # Implement how to log the messages from this module. It helps debugging.
  mattr_accessor :deadlock_logger
  self.deadlock_logger = proc { |msg, klass| klass.logger.warn(msg) if klass.logger }

  # Define how long to wait before retrying a transaction, in milliseconds.
  mattr_accessor :minimum_wait_before_retry
  mattr_accessor :maximum_wait_before_retry

  self.minimum_wait_before_retry = 100
  self.maximum_wait_before_retry = 500

  def self.included(base)
    base.extend(ClassMethods)
    base.class_eval do
      class << self
        alias_method_chain :transaction, :deadlock_handling
      end
    end
  end

  module ClassMethods
    DEADLOCK_ERROR_MESSAGES = [
      "Deadlock found when trying to get lock",
      "Lock wait timeout exceeded"
    ]

    def transaction_with_deadlock_handling(*objects, &block)
      retry_count = 0

      begin
        transaction_without_deadlock_handling(*objects, &block)
      rescue ActiveRecord::StatementInvalid => error
        raise if in_nested_transaction?
        raise unless DEADLOCK_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/i }

        if retry_count >= DeadlockRetry.maximum_retries_on_deadlock
          DeadlockRetry.deadlock_logger.call("Deadlock detected after #{retry_count+1} tries, giving up", self)
          log_innodb_status
          raise
        end

        retry_count += 1

        pause = DeadlockRetry.minimum_wait_before_retry +
          rand(DeadlockRetry.maximum_retries_on_deadlock -
            DeadlockRetry.minimum_wait_before_retry)

        DeadlockRetry.deadlock_logger.call("Deadlock detected on try ##{retry_count}, restarting transaction in #{pause}ms", self)

        sleep(pause / 1000.0)

        retry
      end
    end

    private

    def in_nested_transaction?
      # open_transactions was added in 2.2's connection pooling changes.
      connection.respond_to?(:open_transactions) && connection.open_transactions > 0
    end

    def log_innodb_status
      # `show innodb status` is the only way to get visiblity into why the transaction deadlocked
      lines = connection.select_value("show innodb status")
      DeadlockRetry.deadlock_logger.call("INNODB Status follows:", self)
      lines.each_line { |line| DeadlockRetry.deadlock_logger.call(line, self) }
    rescue Exception => e
      # If it fails, it's not the end of the world. Let's just ignore it.
      DeadlockRetry.deadlock_logger.call("Failed to log innodb status: #{e.message}", self)
    end

  end

end

ActiveRecord::Base.send(:include, DeadlockRetry)
