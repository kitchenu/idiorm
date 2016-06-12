<?php

namespace Idiorm;

use ArrayAccess;
use ArrayIterator;
use Countable;
use IteratorAggregate;
use Serializable;

/**
 * A result set class for working with collections of model instances
 * @author Simon Holywell <treffynnon@php.net>
 */
class ResultSet implements Countable, IteratorAggregate, ArrayAccess, Serializable
{
    /**
     * The current result set as an array
     * @var array
     */
    protected $results = [];

    /**
     * Optionally set the contents of the result set by passing in array
     * @param array $results
     * @return void
     */
    public function __construct(array $results = [])
    {
        $this->setResults($results);
    }

    /**
     * Set the contents of the result set by passing in array
     * @param array $results
     * @return void
     */
    public function setResults(array $results)
    {
        $this->results = $results;
    }

    /**
     * Get the current result set as an array
     * @return array
     */
    public function getResults()
    {
        return $this->results;
    }

    /**
     * Get the current result set as an array
     * @return array
     */
    public function asArray()
    {
        return $this->getResults();
    }

    /**
     * Get the number of records in the result set
     * @return int
     */
    public function count()
    {
        return count($this->results);
    }

    /**
     * Get an iterator for this object. In this case it supports foreaching
     * over the result set.
     * @return ArrayIterator
     */
    public function getIterator()
    {
        return new ArrayIterator($this->results);
    }

    /**
     * ArrayAccess
     * @param int|string $offset
     * @return bool
     */
    public function offsetExists($offset)
    {
        return isset($this->results[$offset]);
    }

    /**
     * ArrayAccess
     * @param int|string $offset
     * @return mixed
     */
    public function offsetGet($offset)
    {
        return $this->results[$offset];
    }

    /**
     * ArrayAccess
     * @param int|string $offset
     * @param mixed $value
     * @return void
     */
    public function offsetSet($offset, $value)
    {
        $this->results[$offset] = $value;
    }

    /**
     * ArrayAccess
     * @param int|string $offset
     * @return void
     */
    public function offsetUnset($offset)
    {
        unset($this->results[$offset]);
    }

    /**
     * Serializable
     * @return string
     */
    public function serialize()
    {
        return serialize($this->results);
    }

    /**
     * Serializable
     * @param string $serialized
     * @return array
     */
    public function unserialize($serialized)
    {
        return unserialize($serialized);
    }

    /**
     * Call a method on all models in a result set. This allows for method
     * chaining such as setting a property on all models in a result set or
     * any other batch operation across models.
     * @example ORM::forTable('Widget')->find_many()->set('field', 'value')->save();
     * @param string $method
     * @param array $params
     * @return \ResultSet
     */
    public function __call($method, $params = [])
    {
        foreach ($this->results as $model) {
            if (method_exists($model, $method)) {
                call_user_func_array([$model, $method], $params);
            } else {
                throw new MethodMissingException("Method $method() does not exist in class " . get_class($this));
            }
        }
        return $this;
    }
}
