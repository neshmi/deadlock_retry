require 'rubygems'

require 'active_record'
require 'active_record/base'
require 'active_record/version'
require 'sqlite3'
puts "Testing ActiveRecord #{ActiveRecord::VERSION::STRING}"

require 'test/unit'
require "#{File.dirname(__FILE__)}/../lib/deadlock_retry"

ActiveRecord::Base.establish_connection :adapter => "sqlite3", :database => ":memory:"
class MockModel < ActiveRecord::Base
end

class DeadlockRetryTest < Test::Unit::TestCase
  DEADLOCK_ERROR = "MySQL::Error: Deadlock found when trying to get lock"
  TIMEOUT_ERROR = "MySQL::Error: Lock wait timeout exceeded"

  def test_no_errors
    assert_equal :success, MockModel.transaction { :success }
  end

  def test_no_errors_with_deadlock
    errors = [ DEADLOCK_ERROR ] * 3
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_no_errors_with_lock_timeout
    errors = [ TIMEOUT_ERROR ] * 3
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_error_if_limit_exceeded
    assert_raise(ActiveRecord::StatementInvalid) do
      MockModel.transaction { raise ActiveRecord::StatementInvalid, DEADLOCK_ERROR }
    end
  end

  def test_error_if_unrecognized_error
    assert_raise(ActiveRecord::StatementInvalid) do
      MockModel.transaction { raise ActiveRecord::StatementInvalid, "Something else" }
    end
  end

  def test_included_by_default
    assert ActiveRecord::Base.respond_to?(:transaction_with_deadlock_handling)
  end

  def test_error_in_nested_transaction_should_retry_outermost_transaction
    tries = 0
    errors = 0

    MockModel.transaction do
      tries += 1
      MockModel.transaction do
        MockModel.transaction do
          errors += 1
          raise ActiveRecord::StatementInvalid, "MySQL::Error: Lock wait timeout exceeded" unless errors > 3
        end
      end
    end
  
    assert_equal 4, tries
  end
end
