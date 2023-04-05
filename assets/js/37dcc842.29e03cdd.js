"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[916],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return u}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var c=a.createContext({}),l=function(e){var t=a.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=l(e.components);return a.createElement(c.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),h=l(n),u=r,m=h["".concat(c,".").concat(u)]||h[u]||d[u]||i;return n?a.createElement(m,o(o({ref:t},p),{},{components:n})):a.createElement(m,o({ref:t},p))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=h;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var l=2;l<i;l++)o[l]=n[l];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},3974:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return c},metadata:function(){return l},toc:function(){return p},default:function(){return h}});var a=n(7462),r=n(3366),i=(n(7294),n(3905)),o=["components"],s={sidebar_position:1},c="Intro",l={unversionedId:"learn-graphql/intro",id:"learn-graphql/intro",title:"Intro",description:"Preface",source:"@site/docs/learn-graphql/intro.md",sourceDirName:"learn-graphql",slug:"/learn-graphql/intro",permalink:"/graphitation/docs/learn-graphql/intro",editUrl:"https://github.com/microsoft/graphitation/tree/main/website/docs/learn-graphql/intro.md",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",next:{title:"The design of GraphQL",permalink:"/graphitation/docs/learn-graphql/the-design-of-graphql"}},p=[{value:"Preface",id:"preface",children:[],level:2},{value:"About",id:"about",children:[],level:2}],d={toc:p};function h(e){var t=e.components,n=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"intro"},"Intro"),(0,i.kt)("h2",{id:"preface"},"Preface"),(0,i.kt)("blockquote",null,(0,i.kt)("p",{parentName:"blockquote"},"GraphQL is a query language for APIs and a runtime for fulfilling those queries with your existing data. GraphQL provides a complete and understandable description of the data in your API."),(0,i.kt)("p",{parentName:"blockquote"},"\u2014 ",(0,i.kt)("a",{parentName:"p",href:"https://graphql.org"},"graphql.org"))),(0,i.kt)("p",null,"While technically accurate, its brevity leaves a lot of room for misguided understanding, typically based on prior experiences."),(0,i.kt)("p",null,"The authors of this guide have observed that ",(0,i.kt)("strong",{parentName:"p"},"the original premise of GraphQL is lost on most users"),". On the one hand this speaks to the versatility of GraphQL, but on the other hand it means that missing key nuances can cause incorrect application of GraphQL, even in the exact same context it was designed for: ",(0,i.kt)("strong",{parentName:"p"},"complex data-driven UI applications"),"."),(0,i.kt)("p",null,"This guide aims to teach you everything you need to understand about GraphQL from that perspective, including how to design schemas, how to implement field resolvers, and how to effectively use this to build these data-driven UIs."),(0,i.kt)("p",null,"This guide does not aim to replace ",(0,i.kt)("a",{parentName:"p",href:"https://graphql.org/learn/"},"the canonical graphql.org site\u2019s documentation"),". Some familiarity with GraphQL might be necessary for some sections, where possible the guide will link to the relevant existing documentation."),(0,i.kt)("div",{className:"admonition admonition-caution alert alert--warning"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"16",height:"16",viewBox:"0 0 16 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M8.893 1.5c-.183-.31-.52-.5-.887-.5s-.703.19-.886.5L.138 13.499a.98.98 0 0 0 0 1.001c.193.31.53.501.886.501h13.964c.367 0 .704-.19.877-.5a1.03 1.03 0 0 0 .01-1.002L8.893 1.5zm.133 11.497H6.987v-2.003h2.039v2.003zm0-3.004H6.987V5.987h2.039v4.006z"}))),"caution")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Each section of this guide builds upon the knowledge gained in the previous section. Not taking this into account means you may end-up repeating the mistakes made in the community to learn about GraphQL outside of the context that it was designed for."),(0,i.kt)("p",{parentName:"div"},"If possible, read the guide from start to finish."))),(0,i.kt)("h2",{id:"about"},"About"),(0,i.kt)("p",null,"GraphQL is a new way of thinking about data and how to access it. It is a data query language that was invented by Facebook in 2012 to make it easier to deal with the complexity of data and state driven UI applications, combined with the scale of their workforce and codebase. GraphQL has since been open-sourced and is now used by many companies that have to deal with the same complexities."),(0,i.kt)("p",null,"GraphQL isn't tied to any specific database or storage engine, instead it is an abstraction over the underlying APIs, expressed using a GraphQL schema. This higher level interface is more convenient for UI developers, as it allows them to access the data in a more consistent and predictable way. Additionally, a good GraphQL schema is focussed on expressing the actual domain models and the semantics thereof, rather than the underlying API data and the many disparate forms it can take. By carefully designing your schema, you can optimize your data retrieval to get exactly the data your UI needs, and ",(0,i.kt)("em",{parentName:"p"},"only")," that data."))}h.isMDXComponent=!0}}]);