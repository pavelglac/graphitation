"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[346],{3905:function(e,t,r){r.d(t,{Zo:function(){return s},kt:function(){return m}});var o=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,o)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,o,n=function(e,t){if(null==e)return{};var r,o,n={},a=Object.keys(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=o.createContext({}),p=function(e){var t=o.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},s=function(e){var t=p(e.components);return o.createElement(c.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},f=o.forwardRef((function(e,t){var r=e.components,n=e.mdxType,a=e.originalType,c=e.parentName,s=i(e,["components","mdxType","originalType","parentName"]),u=p(r),f=n,m=u["".concat(c,".").concat(f)]||u[f]||d[f]||a;return r?o.createElement(m,l(l({ref:t},s),{},{components:r})):o.createElement(m,l({ref:t},s))}));function m(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var a=r.length,l=new Array(a);l[0]=f;var i={};for(var c in t)hasOwnProperty.call(t,c)&&(i[c]=t[c]);i.originalType=e,i[u]="string"==typeof e?e:n,l[1]=i;for(var p=2;p<a;p++)l[p]=r[p];return o.createElement.apply(null,l)}return o.createElement.apply(null,r)}f.displayName="MDXCreateElement"},5735:function(e,t,r){r.r(t),r.d(t,{assets:function(){return c},contentTitle:function(){return l},default:function(){return d},frontMatter:function(){return a},metadata:function(){return i},toc:function(){return p}});var o=r(3117),n=(r(7294),r(3905));const a={id:"howto-models",title:"How-To guide for models",description:"How to properly create models"},l="How-To: Models",i={unversionedId:"learn-graphql/howto/howto-models",id:"learn-graphql/howto/howto-models",title:"How-To guide for models",description:"How to properly create models",source:"@site/docs/learn-graphql/howto/models.md",sourceDirName:"learn-graphql/howto",slug:"/learn-graphql/howto/howto-models",permalink:"/graphitation/docs/learn-graphql/howto/howto-models",draft:!1,editUrl:"https://github.com/microsoft/graphitation/tree/main/website/docs/learn-graphql/howto/models.md",tags:[],version:"current",frontMatter:{id:"howto-models",title:"How-To guide for models",description:"How to properly create models"},sidebar:"tutorialSidebar",previous:{title:"GraphQL Resolution",permalink:"/graphitation/docs/learn-graphql/guides/resolution"},next:{title:"Frequently Asked Questions",permalink:"/graphitation/docs/learn-graphql/faq"}},c={},p=[],s={toc:p},u="wrapper";function d(e){let{components:t,...r}=e;return(0,n.kt)(u,(0,o.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"how-to-models"},"How-To: Models"),(0,n.kt)("p",null,"TODO:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Show a more elaborate example where the field resolver needs data that we clearly do not want to expose in the schema type."),(0,n.kt)("li",{parentName:"ul"},"What logic to put in the model vs resolver, do not convert to schema types in the model")))}d.isMDXComponent=!0}}]);