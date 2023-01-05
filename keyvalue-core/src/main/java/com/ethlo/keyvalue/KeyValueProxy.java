package com.ethlo.keyvalue;

/*-
 * #%L
 * Key-Value - Core
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class KeyValueProxy
{
    public static <T> T proxy(Object target, Class<T> type)
    {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class[]{type},
                new PassThroughHandler(target)
        ));
    }

    public record PassThroughHandler(Object target) implements InvocationHandler
    {
        @Override
        public Object invoke(Object obj, Method method, Object[] args) throws Throwable
        {
            try
            {
                return method.invoke(target, args);
            }
            catch (InvocationTargetException e)
            {
                throw e.getCause();
            }
        }
    }

    public static Object unwrap(Object proxy)
    {
        if (Proxy.isProxyClass(proxy.getClass()))
        {
            final InvocationHandler invocationHandler = Proxy.getInvocationHandler(proxy);
            return ((PassThroughHandler) invocationHandler).target;
        }
        return proxy;
    }
}
