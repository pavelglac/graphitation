"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[397],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return f}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var u=n.createContext({}),c=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(u.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,u=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=c(r),f=a,d=m["".concat(u,".").concat(f)]||m[f]||s[f]||o;return r?n.createElement(d,i(i({ref:t},p),{},{components:r})):n.createElement(d,i({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=m;var l={};for(var u in t)hasOwnProperty.call(t,u)&&(l[u]=t[u]);l.originalType=e,l.mdxType="string"==typeof e?e:a,i[1]=l;for(var c=2;c<o;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},1917:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return l},contentTitle:function(){return u},metadata:function(){return c},toc:function(){return p},default:function(){return m}});var n=r(7462),a=r(3366),o=(r(7294),r(3905)),i=["components"],l={sidebar_position:3,id:"use-fragment",title:"useFragment",slug:"/api-reference/use-fragment/",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",keywords:["fragment","read","fragment reference"]},u=void 0,c={unversionedId:"apollo-react-relay-duct-tape/use-fragment",id:"apollo-react-relay-duct-tape/use-fragment",title:"useFragment",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",source:"@site/docs/apollo-react-relay-duct-tape/use-fragment.md",sourceDirName:"apollo-react-relay-duct-tape",slug:"/api-reference/use-fragment/",permalink:"/graphitation/docs/api-reference/use-fragment/",editUrl:"https://github.com/microsoft/graphitation/tree/main/website/docs/apollo-react-relay-duct-tape/use-fragment.md",tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3,id:"use-fragment",title:"useFragment",slug:"/api-reference/use-fragment/",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",keywords:["fragment","read","fragment reference"]},sidebar:"tutorialSidebar",previous:{title:"useLazyLoadQuery",permalink:"/graphitation/docs/apollo-react-relay-duct-tape/use-lazy-load-query"},next:{title:"useRefetchableFragment",permalink:"/graphitation/docs/api-reference/use-refetchable-fragment/"}},p=[{value:"<code>useFragment</code>",id:"usefragment",children:[{value:"Arguments",id:"arguments",children:[],level:3},{value:"Return Value",id:"return-value",children:[],level:3},{value:"Behavior",id:"behavior",children:[],level:3}],level:2}],s={toc:p};function m(e){var t=e.components,r=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"usefragment"},(0,o.kt)("inlineCode",{parentName:"h2"},"useFragment")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-tsx"},'import React from "react";\nimport {\n  graphql,\n  useFragment,\n} from "@graphitation/apollo-react-relay-duct-tape";\n\nimport { UserComponent_user$key } from "./__generated__/UserComponent_user.graphql";\n\ninterface Props {\n  user: UserComponent_user$key;\n}\n\nconst UserComponent: React.FC<Props> = (props) => {\n  const data = useFragment(\n    graphql`\n      fragment UserComponent_user on User {\n        name\n        profile_picture(scale: 2) {\n          uri\n        }\n      }\n    `,\n    props.user\n  );\n\n  return (\n    <>\n      <h1>{data.name}</h1>\n      <div>\n        <img src={data.profile_picture?.uri} />\n      </div>\n    </>\n  );\n};\n')),(0,o.kt)("h3",{id:"arguments"},"Arguments"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"fragment"),": GraphQL fragment specified using a ",(0,o.kt)("inlineCode",{parentName:"li"},"graphql")," template literal."),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"fragmentReference"),": The ",(0,o.kt)("em",{parentName:"li"},"fragment reference")," is an opaque object that Apollo React/Relay Duct-Tape uses to read the data for the fragment from the store; more specifically, it contains information about which particular object instance the data should be read from.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"The type of the fragment reference can be imported from the generated TypeScript types, from the file ",(0,o.kt)("inlineCode",{parentName:"li"},"<fragment_name>.graphql.ts"),", and can be used to declare the type of your ",(0,o.kt)("inlineCode",{parentName:"li"},"Props"),". The name of the fragment reference type will be: ",(0,o.kt)("inlineCode",{parentName:"li"},"<fragment_name>$key"),".")))),(0,o.kt)("h3",{id:"return-value"},"Return Value"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"data"),": Object that contains data which has been read out from the store; the object matches the shape of specified fragment.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"The TypeScript type for data will also match this shape, and contain types derived from the GraphQL Schema. For example, the type of ",(0,o.kt)("inlineCode",{parentName:"li"},"data")," above is: ",(0,o.kt)("inlineCode",{parentName:"li"},"{ name: string, profile_picture: null | { uri: string } }"),".")))),(0,o.kt)("h3",{id:"behavior"},"Behavior"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"The component is automatically subscribed to updates to the fragment data: if the data for this particular ",(0,o.kt)("inlineCode",{parentName:"li"},"User")," is updated anywhere in the app (e.g. via fetching new data, or mutating existing data), the component will automatically re-render with the latest updated data.")))}m.isMDXComponent=!0}}]);